#!/bin/bash

# Usage
if [ $# -ne 1 ]; then
  echo "Usage: $0 <count>"
  exit 0
fi

# count
count=$1

# Iterate
for i in $(seq 0 $count); do
  echo "Running agent: $i" 
  ./run-entity.sh "agent" 2>&1 > "logs/agentx${i}.log" &
  sleep 0.5
done

