#!/bin/bash

# config
export DEMO_USER="test"
export DEMO_PASS="test"

# run python script
python3 server.py -b 127.0.0.1
# python3 server.py -b <other-network-ips>
# python3 -m http.server 8002
# python3 server_nocache.py


