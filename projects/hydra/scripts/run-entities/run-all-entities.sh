#!/bin/bash

set -x

mkdir -p logs
./run-entity.sh "master" 2>&1 > logs/master.log &
./run-entity.sh "resource_manager" 2>&1 > logs/resource_manager.log &
./run-entity.sh "job_manager" 2>&1 > logs/job_manager.log &
./run-entity.sh "task_manager" 2>&1 > logs/task_manager.log &
./run-entity.sh "swf_manager" 2>&1 > logs/swf_manager.log &
./run-entity.sh "wf_manager" 2>&1 > logs/wf_manager.log &
./run-entity.sh "worker" 2>&1 > logs/worker.log &
./run-entity.sh "agent" 2>&1 > logs/agent.log &
