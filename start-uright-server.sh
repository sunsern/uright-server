#!/usr/bin/env bash

rqworker -q high normal low &
python uright-server.py --port=8000
