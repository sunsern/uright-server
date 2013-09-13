from redis import Redis
from rq import Queue
from datetime import datetime, timedelta
import MySQLdb as mdb
import json
import logging
import numpy as np
from logging import handlers

from uright.clustering import ClusterKMeans
from uright.prototype import PrototypeDTW
from uright.inkutils import INK_STRUCT
from protoset import ProtosetDTW
import mysql_config as mc
import retrain_config as rc

_PU_IDX = INK_STRUCT['PU_IDX']

RACE_MODE_ID = 3

# set up a logger
logger = logging.getLogger("uright.retrain")
formatter = logging.Formatter('%(name)s: %(message)s')
syslog_handler = handlers.SysLogHandler(address='/dev/log')
syslog_handler.setFormatter(formatter)
logger.addHandler(syslog_handler)

def schedule_retrain(user_id, label):
    try:
        con = mdb.connect('localhost', 
                          mc.mysql_username,
                          mc.mysql_password,
                          mc.mysql_database,
                          charset='utf8', 
                          use_unicode=True);

        cur = con.cursor()
        cur.execute("SET NAMES utf8mb4")

        if already_queued(con, user_id, label):
            logger.info("No retrain: (user %s, %s) Already queuing."%(
                    str(user_id), label))
            return

        # dont retrain if not enough new examples
        n_new_examples = count_new_examples(con, user_id, label)
        if (hasTrained(con, user_id, label) and 
            n_new_examples < rc.retrain_frequency):
            logger.info("No retrain: (user %s, %s) too few examples (%d)"%(
                    str(user_id), label, n_new_examples))
            return

        # read user examples
        user_examples = retrieve_user_examples(
            con, user_id, label, 
            max_user_examples=rc.max_user_examples)

        # normalize the user ink
        user_data = None
        if rc.normalization:
            user_data = do_normalize_ink(user_examples)
        else:
            user_data = do_center_ink(user_examples)

        # get best prototype
        prototype_data = closest_prototype(con, user_id, label, user_data)

        # put a record in retrain_queue
        retrain_id = mark_as_queued(con, user_id, label)
        
        ########################
        # Add to retrain queue #
        ########################
        q = Queue('low', connection=Redis())
        q.enqueue(perform_retrain, retrain_id, user_id, 
                  label, user_data, prototype_data)
        
    except:
        logger.exception("An exception has been raised")

    finally:
        if con: con.close()


def perform_retrain(retrain_id, user_id, label, 
                    user_data, prototype_data):
    try:
        con = mdb.connect('localhost', 
                          mc.mysql_username,
                          mc.mysql_password,
                          mc.mysql_database,
                          charset='utf8', 
                          use_unicode=True);

        cur = con.cursor()
        cur.execute("SET NAMES utf8mb4")

        # prepare training data
        combined_data = {'user' : {label : user_data}, 
                         'proto' : {label : prototype_data}}
        
        if len(user_data) == 1 and len(prototype_data) == 0:
            # use the example as prototype
            ps = ProtosetDTW(label, min_cluster_size=0)
            ps.train([zip(user_data,[1.0]*len(user_data))])
        else:
            # run kmeans
            clusterer = ClusterKMeans(
                combined_data,
                target_user_id='proto',
                algorithm='dtw', 
                maxclust=rc.maxclust, 
                equal_total_weight=False,
                target_weight_multiplier=rc.prototype_weight)

            clustered_data = clusterer.clustered_data() 
            # train the prototypes
            ps = ProtosetDTW(label, min_cluster_size=1)
            ps.train(clustered_data[label])

        # add json of protoset to table `protosets`
        if len(ps.trained_prototypes) > 0:
            logger.info("New protoset of %s for user %s"%(label, str(user_id)))
            insert_protoset(con, user_id, label, ps.toJSON())

    except:
        logger.exception('An exception has been raised')

    finally:
        if con: 
            # mark retrain_id as finished. no matter what
            mark_as_finished(con, retrain_id)
            con.commit()
            con.close()


#################################################
# Helper functions
#################################################

def already_queued(con, user_id, label):
    cur = con.cursor(cursorclass=mdb.cursors.DictCursor)
    cur.execute("""
           SELECT * FROM retrain_queue
           WHERE user_id=%s AND label=%s
           ORDER BY added_on DESC
           LIMIT 1;
           """, (user_id, label))
    row = cur.fetchone()
    # not found in the training queue
    if row is None:
        return False
    else:
        # found in the training queue and last entry is not finished
        if row['finished_on'] is None:
            # allow adding to the training queue if 10 minutes passed
            ten_minutes_ago = datetime.now() - datetime.timedelta(minutes=10)
            if row['added_on'] < ten_minutes_ago:
                return False
            else:
                # already in the queue ... 
                return True
        else: 
            return False

def mark_as_queued(con, user_id, label):
    cur = con.cursor()
    cur.execute("""
           INSERT INTO retrain_queue 
             (user_id, label)
           VALUES (%s, %s);
        """, (user_id, label))
    return con.insert_id()
    
def hasTrained(con, user_id, label):
    cur = con.cursor()
    cur.execute("""
           SELECT added_on FROM protosets
           WHERE (user_id=%s AND label=%s)
           ORDER BY protoset_id DESC
           LIMIT 1;
        """, (user_id, label))
    row = cur.fetchone()
    if row is None:
        return False
    else:
        return True

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

def closest_prototype(con, user_id, label, user_data):
    cur = con.cursor()
    cur.execute("""
         SELECT t1.protoset_json
         FROM protosets as t1
         JOIN
            (SELECT MAX(protoset_id) as pid
             FROM protosets 
             WHERE label=%s AND user_id!=%s
             GROUP BY user_id) as t2
         ON t1.protoset_id = t2.pid
        """, (label,user_id,))
    rows = cur.fetchall()
    
    # no prototypes found, return empty
    if len(rows) == 0:
        return []

    # create a list of prototypes
    proto_list = []
    for row in rows:
        protoset_json = json.loads(row[0])
        for prototype_json in protoset_json['prototypes']:
            p = PrototypeDTW(label)
            p.fromJSON(prototype_json)
            proto_list.append(p)

    # select the best prototype
    count = np.zeros(len(proto_list))
    for ink in user_data:
        scores = np.zeros(len(proto_list))
        for i,p in enumerate(proto_list):
            scores[i] = p.score(ink)
        count[scores.argmax()] += 1
    best_proto_ink = proto_list[count.argmax()].model

    # make sure the penup is binary
    best_proto_ink[:,_PU_IDX] = best_proto_ink[:,_PU_IDX].round()
    return [best_proto_ink]

def insert_protoset(con, user_id, label, ps_json):
    ps_type = ps_json['type']
    ps_json_str = json.dumps(ps_json, ensure_ascii=False)
    cur = con.cursor()
    cur.execute("""
           INSERT INTO protosets
              (user_id, label, protoset_type, protoset_json)
           VALUES (%s,%s,%s,%s);
        """, (user_id, label, ps_type, ps_json_str))

def mark_as_finished(con, retrain_id):
    current = datetime.now()
    cur = con.cursor()
    cur.execute("""
           UPDATE retrain_queue 
           SET finished_on=%s
           WHERE retrain_id<=%s AND finished_on is NULL
           """, (current, retrain_id))

def do_normalize_ink(user_raw_ink, timestamp=False, version='uright3'):
    from uright.inkutils import json2array, normalize_ink, filter_bad_ink
    normalized_ink = [
        np.nan_to_num(
            normalize_ink(
                json2array(ink, timestamp=timestamp, version=version))) 
        for ink in filter_bad_ink(user_raw_ink, min_length=1, version=version)]
    return normalized_ink


def do_center_ink(user_raw_ink, timestamp=False, version='uright3'):
    from uright.inkutils import json2array, center_ink, filter_bad_ink
    normalized_ink = [
        np.nan_to_num(center_ink(
                json2array(ink, timestamp=timestamp, version=version))) 
        for ink in filter_bad_ink(user_raw_ink, min_length=1, version=version)]
    return normalized_ink
