#!/bin/bash

BASEDIR="/code"

cd $BASEDIR

# echo "Installing any updated version of the packages"
# ./install.sh

echo "Running jupyter"
jupyter lab --ip 0.0.0.0 --port 8888 --allow-root

