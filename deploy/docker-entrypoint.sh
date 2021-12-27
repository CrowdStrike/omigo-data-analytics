#!/bin/bash

BASEDIR="/code"

cd $BASEDIR

echo "Installing any updated version of the packages"
pip3 install omigo_core omigo_ext --upgrade

echo "Running jupyter"
jupyter lab --ip 0.0.0.0 --port 8888 --allow-root

