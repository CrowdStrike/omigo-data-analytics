#!/bin/bash

set -x

for pid in `ps -ef|grep 'CLUSTER_ENTITY'|grep -v 'grep'|awk '{print $2}'`; do
  echo "Killing $pid"
  kill $pid
done
