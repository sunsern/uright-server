from flask import (Flask, Response, g,
                   request, jsonify,
                   abort, render_template)
from redis import Redis
from rq import Queue
import MySQLdb as mdb
import json
import numpy as np

from process_session import process_session
import mysql_config as mc

RACE_MODE_ID = 3
HISTORY_LENGTH = 10

#####################
# Exp need to level #
#####################
#              Lv.1  Lv.2  Lv.3  Lv.4  Lv.5  Lv.6   Lv.7   Lv.8   Lv.9
_exp_needed = [10.0, 20.0, 30.0, 40.0, 80.0, 160.0, 320.0, 640.0, 1280.0]

app = Flask(__name__)

@app.before_request
def db_connect():
    g.db_con = mdb.connect('localhost', 
                           mc.mysql_username,
                           mc.mysql_password,
                           mc.mysql_database,
                           charset='utf8')
    cur = g.db_con.cursor()
    cur.execute("SET NAMES utf8mb4")

@app.teardown_request
def db_disconnect(exception=None):
    db_con = getattr(g, 'db_con', None)
    if db_con is not None:
        db_con.commit()
        db_con.close()

#########################################

@app.route("/leaderboard")
def leaderboard():
    try:
        con = g.db_con
        
        from datetime import date, timedelta
        today = date.today()
        monday = today - timedelta(days=today.weekday())
        next_monday = monday + timedelta(weeks=1);

        # get best weekly bps
        cur = con.cursor()    
        cur.execute("""
            SELECT t2.displayname, t2.level, COALESCE(MAX(t1.bps),0) AS max_bps
            FROM  sessions AS t1,  users AS t2
            WHERE t1.user_id = t2.user_id
             AND t1.mode_id = %s 
             AND t1.added_on > %s
             AND t2.user_id = t2.linked_id
            GROUP BY t1.user_id
            ORDER BY max_bps DESC
            LIMIT 10
            """,(RACE_MODE_ID,monday))
        rows = cur.fetchall()
        users = []
        for i,row in enumerate(rows):
            rank = i+1
            username = row[0]
            level = row[1]
            bps = row[2]
            if username.startswith('FB_'):
                username = username[3:] + ' (FB)'
            elif username.startswith('PF_'):
                username = username[3:]

            # truncate string to 18 chars
            username = ((username[:18] + '...') if len(username) > 18 
                        else username)

            users.append({'rank' : rank,
                          'username' : username,
                          'level' : level,
                          'bps' : "%0.2f"%bps})

        return render_template('leaderboard.html', 
                               users=users, 
                               reset=next_monday.strftime("%Y/%m/%d"))
    except:
        import traceback; traceback.print_exc()
        abort(400)

#########################################

@app.route("/userstats", methods=['POST'])
def userstats():
    try:
        key = request.form['key']    
        if (key != mc.secret_key): abort(403)

        user_id = request.form['user_id']

        con = g.db_con
        
        # get exp and level
        cur = con.cursor(cursorclass=mdb.cursors.DictCursor)
        cur.execute("""
           SELECT experience,level,next_level_exp 
           FROM users WHERE user_id=%s""",(user_id,))
        resp = cur.fetchone()

        if resp['level'] == 0:
            resp['this_level_exp'] = resp['experience']
        else:
            resp['this_level_exp'] = (resp['experience'] - 
                                      np.sum(_exp_needed[:resp['level']]))
            
        # get best bps
        cur = con.cursor()    
        cur.execute("""
            SELECT COALESCE(MAX(bps),0) FROM sessions 
            WHERE user_id=%s
            """,(user_id,))
        row = cur.fetchone()
        resp['best_bps'] = row[0]

        # get recent bps from race mode
        cur = con.cursor()    
        cur.execute("""
            SELECT bps FROM sessions 
            WHERE user_id=%s
            ORDER BY session_id DESC
            LIMIT %s;
            """,(user_id,
                 HISTORY_LENGTH))
        rows = cur.fetchall()
        resp['recent_bps'] = [row[0] for row in rows]
        resp['recent_bps'].reverse()

        return jsonify(resp)
    except:
        import traceback; traceback.print_exc()
        return jsonify({'ERROR':1})


#########################################

@app.route("/annoucement", methods=['POST'])
def annoucement():
    try:
        key = request.form['key']    
        if (key != mc.secret_key): abort(403)

        con = g.db_con
        cur = con.cursor(cursorclass=mdb.cursors.DictCursor)    
        cur.execute("SELECT * FROM charsets WHERE hidden=0;")

        resp = {}

        import time, datetime
        f = open("annoucement.txt","r")
        raw_text = f.readline()
        if raw_text:
            data = raw_text.split('::')
            if len(data) == 2:
                d = datetime.datetime.strptime(data[0],"%m/%d/%Y").date()
                timestamp = time.mktime(d.timetuple())
                resp = { 'annoucement' : data[1],
                         'timestamp' : timestamp}

        return jsonify(resp)
    except:
        import traceback; traceback.print_exc()
        return jsonify({'ERROR':1})

#########################################

@app.route("/charsets", methods=['POST'])
def charsets():
    try:
        key = request.form['key']    
        if (key != mc.secret_key): abort(403)

        con = g.db_con
        cur = con.cursor(cursorclass=mdb.cursors.DictCursor)    
        cur.execute("SELECT * FROM charsets WHERE hidden=0;")
        
        # [ {'charsetID' : int , 
        #    'name' : string 
        #    'character' : [] } ]
        resp = []
        rows = cur.fetchall()
        for row in rows:
            resp.append(
                {   'charsetID' : row['charset_id'],
                    'name' : row['charset_name'],
                    'characters' : json.loads(row['characters']) 
                    }
                )
        return Response(json.dumps(resp, 
                                   ensure_ascii=False).encode('utf-8'))
    except:
        import traceback; traceback.print_exc()
        return jsonify({'ERROR':1})

#########################################

@app.route("/protosets", methods=['POST'])
def protosets():
    try:
        key = request.form['key']
        if (key != mc.secret_key): abort(403)

        user_id = request.form['user_id']

        con = g.db_con
        cur = con.cursor(cursorclass=mdb.cursors.DictCursor)    

        # initialize with global protosets
        cur.execute("""
             SELECT protoset_id, protoset_json FROM protosets 
             WHERE user_id=%s;
             """, (-1,))

        resp = {}
        rows = cur.fetchall()
        for row in rows:
            protoset = json.loads(row['protoset_json'])
            label = protoset['label']
            protoset['protosetID'] = row['protoset_id']
            resp[label] = protoset
        
        # supplement with user-specific protosets
        cur.execute("""
             SELECT protoset_id, protoset_json FROM protosets 
             WHERE user_id=%s;
             """, (user_id,))

        rows = cur.fetchall()
        for row in rows:
            protoset = json.loads(row['protoset_json'])
            label = protoset['label']
            protoset['protosetID'] = row['protoset_id']
            resp[label] = protoset
        
        return Response(json.dumps(resp, 
                                   ensure_ascii=False).encode('utf-8'))
    except:
        import traceback; traceback.print_exc()
        return jsonify({'ERROR' : 1})
    
#########################################

@app.route("/newuser", methods=['POST'])
def newuser():
    try:
        key = request.form['key']
        if (key != mc.secret_key): abort(403)    

        username = request.form['username']
        fullname = request.form['fullname']
        email = request.form['email']
    
        con = g.db_con       
        cur = con.cursor()
        cur.execute("""
           INSERT INTO users
             (username, fullname, email, displayname, aliases) 
           VALUES (%s,%s,%s,%s,%s);
           """, (username, fullname, email, username[3:], ''))

        resp = {}
        resp['user_id'] = con.insert_id()

        cur = con.cursor()
        cur.execute("""
           UPDATE users 
           SET linked_id=user_id
           WHERE user_id=%s;
           """, (resp['user_id'],))

        return jsonify(resp)

    except:
        import traceback; traceback.print_exc()
        return jsonify({'user_id':0})

#########################################

@app.route("/login", methods=['POST'])
def login():
    try:
        key = request.form['key']
        if (key != mc.secret_key): abort(403)

        username = request.form['username']

        con = g.db_con
        cur = con.cursor(cursorclass=mdb.cursors.DictCursor)
        cur.execute("SELECT user_id FROM users WHERE username=%s;", 
                    (username,))

        resp = {}
        row = cur.fetchone()
        if row is None:
            resp['login_result'] = 'User not found'
        else:
            resp['login_result'] = 'OK'
            resp['user_id'] = row['user_id']

        return jsonify(resp)

    except:
        import traceback; traceback.print_exc()
        return jsonify({'login_result':'ERROR'})

#########################################

def level_by_exp(exp):
    exp_obj = {}
    exp_obj['experience'] = exp
    level = 0
    next_level_exp = 0
    for needed in _exp_needed:
        if exp >= needed:
            level += 1
            exp -= needed
        else:
            next_level_exp = needed - exp
            break
    exp_obj['level'] = level
    exp_obj['next_level_exp'] = next_level_exp
    return exp_obj

@app.route("/upload", methods=['POST'])
def upload():
    try:
        key = request.form['key']
        if (key != mc.secret_key): abort(403)
        
        user_id = request.form['user_id']
        mode_id = request.form['mode_id']
        bps = request.form['bps']
        total_time = request.form['total_time']
        total_score = request.form['total_score']
        active_chars = request.form['active_characters'].encode('utf-8')
        active_pids = request.form['active_protoset_ids'].encode('utf-8')
        session_json = request.form['session_json'].encode('utf-8')

        con = g.db_con 
        cur = con.cursor(cursorclass=mdb.cursors.DictCursor)
        cur.execute("""
          INSERT INTO sessions
            (user_id, mode_id, bps, total_time,
             total_score, session_json, 
             active_protoset_ids, active_characters)
          VALUES
            (%s,%s,%s,%s,%s,%s,%s,%s);
          """, (user_id, mode_id, bps, total_time, 
                total_score, session_json,
                active_pids, active_chars))

        session = json.loads(session_json)
        session['sessionID'] = con.insert_id()
        
        # update exp and level
        if int(mode_id) == RACE_MODE_ID:
            cur = con.cursor()    
            cur.execute("""
               SELECT experience FROM users WHERE user_id=%s
               """,(user_id,))
            row = cur.fetchone()
            exp = level_by_exp(row[0] + float(bps))

            cur = con.cursor()
            cur.execute("""
               UPDATE users 
               SET experience=%s, level=%s, next_level_exp=%s
               WHERE user_id=%s
               """, (exp['experience'],
                     exp['level'],
                     exp['next_level_exp'],
                     user_id))

        ##################################
        # Queue the process_session task #
        ##################################    
        q = Queue('normal', connection=Redis())
        q.enqueue(process_session, session)

        return jsonify({'Error':0})
    except:
        import traceback; traceback.print_exc()
        return jsonify({'Error':1})

#########################################

if __name__ == "__main__":
    import argparse
    import logging
    import logging.handlers as lh
    parser = argparse.ArgumentParser()
    parser.add_argument('--port', default=8000, type=int,
                        help='port to listen (default: 8000)')
    args = parser.parse_args()
    app.logger.setLevel(logging.INFO)
    app.logger.addHandler(lh.SysLogHandler(address='/dev/log'))
    app.run(host='0.0.0.0', port=args.port)
