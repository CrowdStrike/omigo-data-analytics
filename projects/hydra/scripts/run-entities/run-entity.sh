#!/bin/bash

# command line parameters
if [ $# != 2 ]; then
    echo "Usage: $0 <entity-type> <entity-marker>"
    exit 0
fi

# read parameters
ENTITY_TYPE="$1"
ENTITY_MARKER="$2"

# run
python3 run-entity.py "$ENTITY_TYPE" "$ENTITY_MARKER"
