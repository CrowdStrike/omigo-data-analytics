#!/bin/bash

# command line parameters
if [ $# != 1 ]; then
    echo "Usage: $0 <entity-type>"
    exit 0
fi

# read parameters
ENTITY_TYPE="$1"

# run
python3 run-entity.py "$ENTITY_TYPE" "CLUSTER_ENTITY"
