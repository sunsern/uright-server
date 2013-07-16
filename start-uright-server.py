#!/usr/bin/env bash

rqworker -q &
python uright-server.py --port=8000
