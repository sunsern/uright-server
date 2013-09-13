#!/usr/bin/env bash

zdaemon -p "rqworker normal" -d -s zdsock-rq-normal start
zdaemon -p "rqworker low" -d -s zdsock-rq-low start
zdaemon -p "python uright-server.py --port=8001" -d -s zdsock-uright1 start
zdaemon -p "python uright-server.py --port=8002" -d -s zdsock-uright2 start
