import MySQLdb as mdb
import mysql_config as mc
import json
import sys

def process_session(session):
    try:
        con = mdb.connect('localhost', 
                          mc.mysql_username,
                          mc.mysql_password,
                          mc.mysql_database,
                          charset='utf8');

        user_id = session['userID']
        language_id = session['languageID']
        session_id = session['sessionID']
        mode_id = session['modeID']
        
        for _round in session['rounds']:
            start_time = _round['startTime']
            score = _round['score']
            penup_time = _round['lastPenupTime']
            pendown_time = _round['firstPendownTime']
            label = _round['label']

            result = _round['result']
            if result:
                predicted = max(result, key=result.get)
            else:
                predicted = ''

            result = json.dumps(result)
            ink = json.dumps(_round['ink'])
            cur = con.cursor()
            cur.execute("""
               INSERT INTO inkdata
                 (user_id, language_id, session_id, mode_id,
                  label, predicted, ink, start_time, 
                  pendown_time, penup_time, attempt)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
               """,  (user_id, language_id, session_id, mode_id,
                      label, predicted, ink, start_time, 
                      pendown_time, penup_time, 0))
    except Exception as ex:
        print "Something went wrong ... "
        print sys.exc_info()[0]
    finally:
        if con: con.close()
