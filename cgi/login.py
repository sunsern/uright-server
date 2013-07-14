#!/usr/bin/env python

import mysql_config as mc
import MySQLdb as mdb
import cgi
import json
    
def authenticate(con, username, password):
    cur = con.cursor(cursorclass=mdb.cursors.DictCursor)
    cur.execute("SELECT * FROM users WHERE username=\'%s\'"%username)
    row = cur.fetchone()
    if row is None:
        return (-1, 'User not found')
    elif (password == row['password']):
        return (row['user_id'], 'OK')
    else:
        return (-1, 'Incorrect password')
    
def languages(con):
    json_obj = {}
    cur = con.cursor(cursorclass=mdb.cursors.DictCursor)
    cur.execute("SELECT * FROM languages WHERE hidden=0")
    rows = cur.fetchall()
    for row in rows:
        json_obj["%d"%(row['language_id'])] = {
            'id':row['language_id'],
            'name':row['language_name'],
            'character':json.loads(row['characters'])}
    return json_obj

def setUTF8(con):
    cur = con.cursor(cursorclass=mdb.cursors.DictCursor)
    cur.execute("SET NAMES \'utf8\'")
  
def main():
    form = cgi.FieldStorage()
    key = form.getvalue('key')
    username = form.getvalue('username')
    password = form.getvalue('password')

    if (key != mc.secret_key):
        return


    try:
        con = mdb.connect('localhost', 
                          mc.mysql_username,
                          mc.mysql_password,
                          mc.mysql_database);

        print "Content-Type: application/json\n"

        setUTF8(con)

        json_obj = {}
        json_obj['languages'] = languages(con)

        user_id, login_result = authenticate(con, username, password)

        json_obj['login_result'] = login_result
        json_obj['user_id'] = user_id

        print json.dumps(json_obj, ensure_ascii=False).encode('utf-8')

    except mdb.Error, e:
        print "Content-Type: text/html\n"
        print "Error %d: %s" % (e.args[0],e.args[1])

    finally:    
        if con:    
            con.close()


if __name__ == "__main__":
    main()
