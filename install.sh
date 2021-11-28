#!/bin/bash

echo "Updating the python build module"
python3 -m pip install --upgrade build

echo "Building core package"
cd python-packages/core
python3 -m build

echo "Installing core package"
pip3 install dist/tsv_data_analytics-*.tar.gz

echo "Building extensions package"
cd -
cd python-packages/extensions
python3 -m build

echo "Installing extensions package"
pip3 install dist/tsv_data_analytics_ext-*.tar.gz

