#!/usr/local/bin/bash

# check command line arguments
if [ $# -ne 1 ]; then
  echo "Usage: $0 <capabilities>"
  exit 0
fi

# set -x

# create a map for already rsynced wf_ids
declare -A created_wf_ids_map
declare -A non_created_wf_ids_map

# read capabilities
CAPABILITIES=$1
REMOTE_HOST="$PROXY_REMOTE_HOST"

# bi directional. this is problematic. TODO
while true; do
  # look for new wf
  for created_wf_id in `python3 ./get-local-created-wf-ids.py $CAPABILITIES`; do
    if [ ! -v created_wf_ids_map[$created_wf_id] ]; then 
      echo "Processing local created wf: created_wf_id: $created_wf_id"
      rsync -a --delete rsync-dir/entities-details/wfs/$created_wf_id $REMOTE_HOST:~/hydra/rsync-dir/entities-details/wfs
      rsync -a --delete rsync-dir/entities-state/wfs/created/$created_wf_id $REMOTE_HOST:~/hydra/rsync-dir/entities-state/wfs/created
      rsync -a --delete rsync-dir/entities-data/wfs/$created_wf_id $REMOTE_HOST:~/hydra/rsync-dir/entities-data/wfs
      rsync -a --delete rsync-dir/entities-incoming/wfs/$created_wf_id $REMOTE_HOST:~/hydra/rsync-dir/entities-incoming/wfs
      rsync -a --delete rsync-dir/entities-ids/wfs/$created_wf_id $REMOTE_HOST:~/hydra/rsync-dir/entities-ids/wfs

      # make an entry into created_wf_ids_map
      created_wf_ids_map[$created_wf_id]="1"
    fi
  done

  # sync the output data for created wf ids
  for created_wf_id in `python3 ./get-local-created-wf-ids.py $CAPABILITIES`; do
    echo "Processing remote data: created_wf_id: $created_wf_id"
    # entities-state is completely passive
    # rsync -a --delete $REMOTE_HOST:~/hydra/rsync-dir/entities-data/wfs/$created_wf_id/outputs rsync-dir/entities-data/wfs/$created_wf_id

    # TODO: this is called multiple times
    rsync -a --delete $REMOTE_HOST:~/hydra/rsync-dir/entities-state/wfs/completed rsync-dir/entities-state/wfs
    rsync -a --delete $REMOTE_HOST:~/hydra/rsync-dir/entities-state/wfs/failed rsync-dir/entities-state/wfs
    rsync -a --delete $REMOTE_HOST:~/hydra/rsync-dir/entities-state/wfs/aborted rsync-dir/entities-state/wfs

    # echo "Sleeping for 30 seconds"
    # sleep 30

    # call only once
    break
  done

  # sync the final status for non_created_wf_id
  for non_created_wf_id in `python3 ./get-local-non-created-wf-ids.py $CAPABILITIES`; do
    if [ ! -v non_created_wf_ids_map[$non_created_wf_id] ]; then 
      echo "Processing non_created_wf_id: $non_created_wf_id"
      # rsync -a --delete $REMOTE_HOST:~/hydra/rsync-dir/entities-details/wfs/$non_created_wf_id rsync-dir/entities-details/wfs
      # rsync -a --delete $REMOTE_HOST:~/hydra/rsync-dir/entities-incoming/wfs/$non_created_wf_id rsync-dir/wfs
      rsync -a --delete $REMOTE_HOST:~/hydra/rsync-dir/entities-data/wfs/$non_created_wf_id/outputs rsync-dir/entities-data/wfs/$non_created_wf_id
      # rsync -a --delete $REMOTE_HOST:~/hydra/rsync-dir/entities-ids/wfs/$non_created_wf_id rsync-dir/entities-ids/wfs

      # make an entry into created_wf_ids_map
      non_created_wf_ids_map[$non_created_wf_id]="1"

      # echo "Sleeping for 10 seconds"
      # sleep 10 
    fi
  done

  echo "Sleeping for 10 seconds"
  sleep 10
done

