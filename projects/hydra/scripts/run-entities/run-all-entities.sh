#!/bin/bash

set -x

# create log directory
mkdir -p logs

# Master handles MASTER and RM incoming only
sleep 2
./run-entity.sh "master" 2>&1 > logs/master01.log &

sleep 2
./run-entity.sh "master" 2>&1 > logs/master02.log &

sleep 2
./run-entity.sh "master" 2>&1 > logs/master03.log &

# RM runs shard election + shard incoming assignment
sleep 2
./run-entity.sh "resource_manager" 2>&1 > logs/resource_manager01.log &

sleep 2
./run-entity.sh "resource_manager" 2>&1 > logs/resource_manager02.log &

sleep 2
./run-entity.sh "resource_manager" 2>&1 > logs/resource_manager03.log &

sleep 2
./run-entity.sh "resource_manager" 2>&1 > logs/resource_manager04.log &

sleep 2
./run-entity.sh "resource_manager" 2>&1 > logs/resource_manager05.log &

# ./run-entity.sh "job_manager" 2>&1 > logs/job_manager.log &
# ./run-entity.sh "task_manager" 2>&1 > logs/task_manager.log &
# ./run-entity.sh "swf_manager" 2>&1 > logs/swf_manager.log &

# SWF_MANAGER now delegates to agents (capacity=10, non-blocking)
sleep 2
./run-entity.sh "swf_manager" 2>&1 > logs/swf_manager01.log &

sleep 2
./run-entity.sh "swf_manager" 2>&1 > logs/swf_manager02.log &

sleep 2
./run-entity.sh "swf_manager" 2>&1 > logs/swf_manager03.log &

sleep 2
./run-entity.sh "swf_manager" 2>&1 > logs/swf_manager04.log &

# WF_MANAGER now delegates to agents (capacity=10, non-blocking)
sleep 2
./run-entity.sh "wf_manager" 2>&1 > logs/wf_manager01.log &

sleep 2
./run-entity.sh "wf_manager" 2>&1 > logs/wf_manager02.log &

sleep 2
./run-entity.sh "wf_manager" 2>&1 > logs/wf_manager03.log &

sleep 2
./run-entity.sh "wf_manager" 2>&1 > logs/wf_manager04.log &

sleep 2
./run-entity.sh "wf_manager" 2>&1 > logs/wf_manager05.log &

# V2: Agents are required - they execute WFs delegated by WF_MANAGER
sleep 2
./run-entity.sh "agent" 2>&1 > logs/agent01.log &

sleep 2
./run-entity.sh "agent" 2>&1 > logs/agent02.log &

sleep 2
./run-entity.sh "agent" 2>&1 > logs/agent03.log &

sleep 2
./run-entity.sh "agent" 2>&1 > logs/agent04.log &

sleep 2
./run-entity.sh "agent" 2>&1 > logs/agent05.log &

sleep 2
./run-entity.sh "agent" 2>&1 > logs/agent06.log &

sleep 2
./run-entity.sh "agent" 2>&1 > logs/agent07.log &

sleep 2
./run-entity.sh "agent" 2>&1 > logs/agent08.log &

sleep 2
./run-entity.sh "agent" 2>&1 > logs/agent09.log &

sleep 2
./run-entity.sh "agent" 2>&1 > logs/agent10.log &

sleep 2
./run-entity.sh "agent" 2>&1 > logs/agent11.log &

sleep 2
./run-entity.sh "agent" 2>&1 > logs/agent12.log &

sleep 2
./run-entity.sh "agent" 2>&1 > logs/agent13.log &

sleep 2
./run-entity.sh "agent" 2>&1 > logs/agent14.log &

sleep 2
./run-entity.sh "agent" 2>&1 > logs/agent15.log &

sleep 2
./run-entity.sh "agent" 2>&1 > logs/agent16.log &

sleep 2
./run-entity.sh "agent" 2>&1 > logs/agent17.log &

sleep 2
./run-entity.sh "agent" 2>&1 > logs/agent18.log &

sleep 2
./run-entity.sh "agent" 2>&1 > logs/agent19.log &

sleep 2
./run-entity.sh "agent" 2>&1 > logs/agent20.log &

sleep 2
./run-entity.sh "agent" 2>&1 > logs/agent21.log &

sleep 2
./run-entity.sh "agent" 2>&1 > logs/agent22.log &

sleep 2
./run-entity.sh "agent" 2>&1 > logs/agent23.log &

sleep 2
./run-entity.sh "agent" 2>&1 > logs/agent24.log &

sleep 2
./run-entity.sh "agent" 2>&1 > logs/agent25.log &

sleep 2
./run-entity.sh "worker" 2>&1 > logs/worker.log &

# Message bus agent — runs ETL rollup each cycle (single instance only)
sleep 2
./run-entity.sh "message_bus_agent" 2>&1 > logs/message_bus_agent01.log &
