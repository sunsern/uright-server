import mysql_config as mc
import MySQLdb as mdb
from flask import Flask
from flask import request
from flask import jsonify
from flask import Response
import argparse
import json

app = Flask(__name__)

@app.route("/languages", methods=['POST'])
def languages():
    key = request.form['key']

    if (key != mc.secret_key): return error

    try:
        con = mdb.connect('localhost', 
                          mc.mysql_username,
                          mc.mysql_password,
                          mc.mysql_database,
                          charset='utf8');

        cur = con.cursor(cursorclass=mdb.cursors.DictCursor)    
        cur.execute("SELECT * FROM languages WHERE hidden=0;")

        json_obj = {}
        rows = cur.fetchall()
        for row in rows:
            json_obj["%d"%(row['language_id'])] = {
                'id':row['language_id'],
                'name':row['language_name'],
                'characters':json.loads(row['characters'])}

        return Response(json.dumps(json_obj, 
                                   ensure_ascii=False).encode('utf-8'))

    except mdb.Error, e:
        return jsonify({})

    finally:    
        if con: con.close()
    

@app.route("/newuser", methods=['POST'])
def newuser():
    key = request.form['key']
    username = request.form['username']
    password = request.form['password']
    fullname = request.form['fullname']
    email = request.form['email']

    if (key != mc.secret_key): return error

    try:
        con = mdb.connect('localhost', 
                          mc.mysql_username,
                          mc.mysql_password,
                          mc.mysql_database,
                          charset='utf8');
        
        cur = con.cursor(cursorclass=mdb.cursors.DictCursor)
        cur.execute("""
           INSERT INTO users
             (username,password,fullname,email) 
           VALUE (%s,%s,%s,%s);""", (username,
                                     password,
                                     fullname,
                                     email))
        user_id = con.insert_id()
        return jsonify({'user_id':user_id})

    except mdb.Error, e:
        return jsonify({'user_id':0})

    
    finally:    
        if con: con.close()


@app.route("/login", methods=['POST'])
def login():
    key = request.form['key']
    username = request.form['username']
    password = request.form['password']
    
    if (key != mc.secret_key): return error

    try:
        con = mdb.connect('localhost', 
                          mc.mysql_username,
                          mc.mysql_password,
                          mc.mysql_database,
                          charset='utf8');

        cur = con.cursor(cursorclass=mdb.cursors.DictCursor)
        cur.execute("SELECT * FROM users WHERE username=%s;", 
                    (username,))
        row = cur.fetchone()
        resp = {}
        if row is None:
            resp['login_result'] = 'User not found'
        elif (password == row['password']):
            resp['login_result'] = 'OK'
            resp['user_id'] = row['user_id']
        else:
            resp['login_result'] = 'Incorrect password'

        return jsonify(resp)

    except mdb.Error, e:
        return jsonify({'login_result':'ERROR'})

    finally:    
        if con: con.close()


@app.route("/upload", methods=['POST'])
def upload():
    key = request.form['key']
    uid = request.form['user_id']
    mid = request.form['mode_id']
    lid = request.form['language_id']
    cid = request.form['classifier_id']
    bps = request.form['bps']
    ttime = request.form['total_time']
    tscore = request.form['total_score']
    raw_json = request.form['raw_json'].encode('utf-8')

    if (key != mc.secret_key): return error

    try:
        con = mdb.connect('localhost', 
                          mc.mysql_username,
                          mc.mysql_password,
                          mc.mysql_database,
                          charset='utf8');

        cur = con.cursor(cursorclass=mdb.cursors.DictCursor)
        cur.execute("""
          INSERT INTO sessions
            (user_id, mode_id, language_id,
             classifier_id, bps, total_time,
             total_score, raw_json)
          VALUES
            (%s,%s,%s,%s,%s,%s,%s,%s);""",
                    (uid,
                     mid,
                     lid,
                     cid,
                     bps,
                     ttime,
                     tscore,
                     raw_json))
        return jsonify({'Error':0})

    except mdb.Error, e:
        return jsonify({'Error':1})

    finally:    
        if con: con.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--port', default=8000, type=int,
                        help='port to listen (default: 8000)')
    args = parser.parse_args()
    app.run(debug=True, host='0.0.0.0', port=args.port)
