#!/usr/bin/env bash

zdaemon -p "python uright-server.py" -d -s zdsock-uright1 stop
zdaemon -p "python uright-server.py" -d -s zdsock-uright2 stop
zdaemon -p "rqworker" -d -s zdsock-rq-normal stop
zdaemon -p "rqworker" -d -s zdsock-rq-low stop
