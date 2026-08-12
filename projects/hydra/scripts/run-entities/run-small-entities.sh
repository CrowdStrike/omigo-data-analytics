#!/bin/bash

set -x

# create log directory
mkdir -p logs

# Master handles MASTER and RM incoming only
./run-entity.sh "master" 2>&1 > logs/master01.log &

# RM runs shard election + shard incoming assignment
./run-entity.sh "resource_manager" 2>&1 > logs/resource_manager01.log &

sleep 0.5
./run-entity.sh "message_bus_agent" 2>&1 > logs/message_bus_agent01.log &

./run-entity.sh "swf_manager" 2>&1 > logs/swf_manager01.log &

./run-entity.sh "wf_manager" 2>&1 > logs/wf_manager05.log &

./run-only-agents.sh 100 
