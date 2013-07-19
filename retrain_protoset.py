from redis import Redis
from rq import Queue
from datetime import datetime
import MySQLdb as mdb
import json
import traceback
import numpy as np

from uright.clustering import ClusterKMeans
from protoset import ProtosetDTW
import mysql_config as mc
import retrain_config as rc

RACE_MODE_ID = 3

def schedule_retrain(user_id, label):
    try:
        con = mdb.connect('localhost', 
                          mc.mysql_username,
                          mc.mysql_password,
                          mc.mysql_database,
                          charset='utf8', 
                          use_unicode=True);
        
        if already_queued(con, user_id, label):
            print "No retrain: Already queuing."
            return

        # dont retrain if not enough new examples
        n_new_examples = count_new_examples(con, user_id, label)
        if (n_new_examples < rc.min_new_examples):
            print "No retrain: too few examples (%d)"%n_new_examples
            return

        # read user examples
        user_examples = retrieve_user_examples(
            con, user_id, label, 
            max_user_examples=rc.max_user_examples)

        # read other examples
        # TODO: create a static pool or something
        other_examples = retrieve_other_examples(
            con, user_id, label, 
            max_other_examples=rc.max_other_examples)
        
        # put a record in retrain_queue
        retrain_id = mark_as_queued(con, user_id, label)
        
        ########################
        # Add to retrain queue #
        ########################
        q = Queue('low', connection=Redis())
        q.enqueue(perform_retrain, retrain_id, user_id, 
                  label, user_examples, other_examples)
        
    except:
        print '-'*60
        traceback.print_exc()
        print '-'*60

    finally:
        if con: con.close()


def perform_retrain(retrain_id, user_id, label, 
                    user_examples, other_examples):
    try:
        con = mdb.connect('localhost', 
                          mc.mysql_username,
                          mc.mysql_password,
                          mc.mysql_database,
                          charset='utf8', 
                          use_unicode=True);

        # prepare training data
        user_data = {label : user_examples}
        other_data = {label : other_examples}
        combined_data = {'user' : user_data, 
                         'other' : other_data}

        # normalize ink?
        if rc.normalization:
            normalized_data = do_normalize_ink(combined_data)
        else:
            normalized_data = do_center_ink(combined_data)


        # run kmeans
        clusterer = ClusterKMeans(normalized_data,
                                  target_user_id='user',
                                  algorithm='dtw', 
                                  maxclust=rc.maxclust, 
                                  equal_total_weight=False,
                                  target_weight_multiplier=rc.target_weight)

        clustered_data = clusterer.clustered_data()                    

        # train the prototypes
        ps = ProtosetDTW(label, min_cluster_size=rc.min_cluster_size)
        ps.train(clustered_data[label])

        # add json of protoset to table `protosets`
        insert_protoset(con, user_id, label, ps.toJSON())

        # mark retrain_id as finished
        mark_as_finished(con, retrain_id)

    except:
        print '-'*60
        traceback.print_exc()
        print '-'*60

    finally:
        if con: con.close()


#################################################
# Helper functions
#################################################

def already_queued(con, user_id, label):
    cur = con.cursor()
    cur.execute("""
           SELECT retrain_id FROM retrain_queue
           WHERE user_id=%s AND label=%s AND finished_on=NULL;
           """, (user_id, label))
    row = cur.fetchone()
    return (row is not None)

def mark_as_queued(con, user_id, label):
    cur = con.cursor()
    cur.execute("""
           INSERT INTO retrain_queue 
             (user_id, label)
           VALUES (%s, %s);
        """, (user_id, label))
    return con.insert_id()
    
def count_new_examples(con, user_id, label):
    cur = con.cursor()
    cur.execute("""
           SELECT added_on FROM protosets
           WHERE (user_id=%s AND label=%s)
           ORDER BY protoset_id DESC
           LIMIT 1;
        """, (user_id, label))
    row = cur.fetchone()
    if row is None:
        # no protoset for the label found
        last_train = datetime(2000,1,1)
    else:
        last_train = row[0]

    # count number of example since last train
    cur = con.cursor()
    cur.execute("""
           SELECT COUNT(*) FROM inkdata
           WHERE (user_id=%s AND label=%s AND 
                  mode_id=%s AND added_on > %s);
        """, (user_id, label, RACE_MODE_ID, last_train))
    row = cur.fetchone()
    return row[0]

def retrieve_user_examples(con, user_id, label, 
                           max_user_examples=30):
    cur = con.cursor()
    cur.execute("""
           SELECT ink_json FROM inkdata
           WHERE (user_id=%s AND label=%s AND 
                  mode_id=%s)
           ORDER BY ink_id DESC
           LIMIT %s;
        """, (user_id, label, RACE_MODE_ID, max_user_examples))
    rows = cur.fetchall()
    return [json.loads(row[0]) for row in rows]

def retrieve_other_examples(con, user_id, label, 
                            max_other_examples=100):
    cur = con.cursor()
    cur.execute("""
           SELECT ink_json FROM inkdata
           WHERE (user_id!=%s AND label=%s AND 
                  mode_id=%s)
           ORDER BY ink_id DESC
           LIMIT %s;
        """, (user_id, label, RACE_MODE_ID, max_other_examples))
    rows = cur.fetchall()
    return [json.loads(row[0]) for row in rows]

def insert_protoset(con, user_id, label, ps_json):
    ps_type = ps_json['type']
    ps_json_str = json.dumps(ps_json)
    cur = con.cursor()
    cur.execute("""
           INSERT INTO protosets
              (user_id, label, protoset_type, protoset_json)
           VALUES (%s,%s,%s,%s);
        """, (user_id, label, ps_type, ps_json_str))
    con.commit()

def mark_as_finished(con, retrain_id):
    current = datetime.now()
    cur = con.cursor()
    cur.execute("""
           UPDATE retrain_queue 
           SET finished_on=%s
           WHERE retrain_id=%s
           """, (current, retrain_id))
    con.commit()

def do_normalize_ink(user_raw_ink, timestamp=False, version='uright3'):
    from uright.inkutils import json2array, normalize_ink, filter_bad_ink
    normalized_ink = {}
    for userid, raw_ink in user_raw_ink.iteritems():
        temp = {}
        for label, ink_list in raw_ink.iteritems():
            temp[label] = [
                np.nan_to_num(normalize_ink(
                        json2array(ink, timestamp=timestamp, version=version))) 
                for ink in filter_bad_ink(ink_list, version=version)]
        normalized_ink[userid] = temp
    return normalized_ink


def do_center_ink(user_raw_ink, timestamp=False, version='uright3'):
    from uright.inkutils import json2array, center_ink, filter_bad_ink
    normalized_ink = {}
    for userid, raw_ink in user_raw_ink.iteritems():
        temp = {}
        for label, ink_list in raw_ink.iteritems():
            temp[label] = [
                np.nan_to_num(center_ink(
                        json2array(ink, timestamp=timestamp, version=version))) 
                for ink in filter_bad_ink(ink_list, version=version)]
        normalized_ink[userid] = temp
    return normalized_ink



