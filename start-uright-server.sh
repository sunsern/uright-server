#!/usr/bin/env bash

zdaemon -p "rqworker -q high" -d -s zdsock-rq-high start
zdaemon -p "rqworker -q normal" -d -s zdsock-rq-normal start
zdaemon -p "rqworker -q low" -d -s zdsock-rq-low start
zdaemon -p "python uright-server.py --port=8001" -d -s zdsock-uright1 start
zdaemon -p "python uright-server.py --port=8002" -d -s zdsock-uright2 start
