#!/usr/bin/env bash

mongod --fork --syslog
cd /opt/datacoll
python3 datacoll.py
