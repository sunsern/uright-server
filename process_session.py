import MySQLdb as mdb
import datetime
import json
import logging
from logging import handlers

from retrain_protoset import schedule_retrain
import mysql_config as mc

# set up a logger
logger = logging.getLogger("uright.processor")
formatter = logging.Formatter('%(name)s: %(message)s')
syslog_handler = handlers.SysLogHandler(address='/dev/log')
syslog_handler.setFormatter(formatter)
logger.addHandler(syslog_handler)

def process_session(session):
    try:
        con = mdb.connect('localhost', 
                          mc.mysql_username,
                          mc.mysql_password,
                          mc.mysql_database,
                          charset='utf8');

        user_id = session['userID']
        mode_id = session['modeID']
        session_id = session['sessionID']

        cur = con.cursor()
        cur.execute("SET NAMES utf8mb4")

        # insert each ink into `inkdata`
        for _round in session['rounds']:
            process_round(con, user_id, session_id,
                          mode_id, _round)

        # mark the session as processed
        current_time = datetime.datetime.now()
        cur = con.cursor()
        cur.execute("""
             UPDATE sessions 
             SET processed_on=%s
             WHERE session_id=%s;
             """, (current_time, session_id))

        con.commit()
        logger.info("Processed a session from user %d"%(user_id))

    except:
        logger.exception('An exception has been raised')

    finally:
        if con: con.close()


def process_round(con, user_id, session_id, mode_id, _round):
    label = _round['label']
    score = _round['score']
    start_time = _round['startTime']
    penup_time = _round['lastPenupTime']
    pendown_time = _round['firstPendownTime']
    prediction_json = json.dumps(_round['result'], ensure_ascii=False)
    ink_json = json.dumps(_round['ink'], ensure_ascii=False)

    cur = con.cursor()
    cur.execute("""
           INSERT INTO inkdata
              (user_id, session_id, mode_id,
               label, prediction_json, ink_json, 
               start_time, pendown_time, penup_time, score)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
           """,  (user_id, session_id, mode_id,
                  label, prediction_json, ink_json, 
                  start_time, pendown_time, penup_time, score))

    # schedule a retraining for this user-label pair
    schedule_retrain(user_id, label) 
