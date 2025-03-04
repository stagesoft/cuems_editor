#!/usr/bin/env bash
cd /opt/app
python3 ws-server.py &
httpd-foreground