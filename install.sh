#!/bin/bash

echo "Updating the python build module"
python3 -m pip install --upgrade build

echo "Building core package"
cd python-packages/core
python3 -m build

echo "Installing core package"
pip3 install dist/omigo_core-0.4.7.tar.gz

echo "Building extensions package"
cd -
cd python-packages/extensions
python3 -m build

echo "Installing extensions package"
pip3 install dist/omigo_ext-0.4.7.tar.gz

