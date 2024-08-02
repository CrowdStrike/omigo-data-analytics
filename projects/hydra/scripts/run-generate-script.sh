#!/bin/bash

# check command line arguments
if [ $# -ne 1 ]; then
  echo "Usage: $0 <capabilities>"
  exit 0
fi

# Command line arguments  
CAPABILITIES=$1

# run this in a loop
while true; do
  # get the list of wf ids
  alive_wf_ids=`python3 ./get-alive-wf-ids.py $CAPABILITIES`

  # iterate over each wf id
  for wf_id in $alive_wf_ids; do
     WF_FILE="wf-script-$alive_wf_ids.sh"

     echo "Creating wf executable script: $wf_id"
     python3 ./get-shell-executor-script.py $wf_id > $WF_FILE

     echo "Setting executable permission"
     chmod +x $WF_FILE

     echo "Executing File: $WF_FILE"
     ./${WF_FILE}

     # status code
     status_code=$?

     # debug
     echo "Exit Status Code: $status_code"

     # deleting the script
     rm -f $WF_FILE

     # sleep
     sleep 30
  done

  # sleep
  echo "Sleeping for 60 seconds..."
  sleep 60
done
