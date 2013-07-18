from flask import (Flask, Response, request, jsonify, g)
from redis import Redis
from rq import Queue
import MySQLdb as mdb
import argparse
import json

from process_session import process_session
import mysql_config as mc

app = Flask(__name__)

@app.before_request
def db_connect():
    g.db_con = mdb.connect('localhost', 
                           mc.mysql_username,
                           mc.mysql_password,
                           mc.mysql_database,
                           charset='utf8');

@app.teardown_request
def db_disconnect(exception=None):
    db_con = getattr(g, 'db_con', None)
    if db_con is not None:
        db_con.close()

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
        return jsonify({'ERROR' : 1})
    
#########################################

@app.route("/newuser", methods=['POST'])
def newuser():
    try:
        key = request.form['key']
        if (key != mc.secret_key): abort(403)    

        username = request.form['username']
        password = request.form['password']
        fullname = request.form['fullname']
        email = request.form['email']
    
        con = g.db_con       
        cur = con.cursor()
        cur.execute("""
           INSERT INTO users
             (username, password, fullname, email) 
           VALUES (%s,%s,%s,%s);
           """, (username, password, fullname, email))

        resp = {}
        resp['user_id'] = con.insert_id()
        return jsonify(resp)

    except:
        return jsonify({'user_id':0})

#########################################

@app.route("/login", methods=['POST'])
def login():
    try:
        key = request.form['key']
        if (key != mc.secret_key): abort(403)

        username = request.form['username']
        password = request.form['password']

        con = g.db_con
        cur = con.cursor(cursorclass=mdb.cursors.DictCursor)
        cur.execute("SELECT user_id, password FROM users WHERE username=%s;", 
                    (username,))

        resp = {}
        row = cur.fetchone()
        if row is None:
            resp['login_result'] = 'User not found'
        elif (password != row['password']):
            resp['login_result'] = 'Incorrect password'
        else:
            resp['login_result'] = 'OK'
            resp['user_id'] = row['user_id']

        return jsonify(resp)

    except:
        return jsonify({'login_result':'ERROR'})

#########################################

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

        ##################################
        # Queue the process_session task #
        ##################################    
        q = Queue('normal', connection=Redis())
        q.enqueue(process_session, session)

        return jsonify({'Error':0})
    except:
        return jsonify({'Error':1})

#########################################

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--port', default=8000, type=int,
                        help='port to listen (default: 8000)')
    args = parser.parse_args()
    app.run(host='0.0.0.0', port=args.port)
