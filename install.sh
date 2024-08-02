#!/bin/bash

echo "Updating the python build module"
python3 -m pip install --upgrade build

echo "Building core package"
cd python-packages/core
rm -f dist/*gz
python3 -m build

echo "Installing core package"
pip3 install dist/omigo_core-*.tar.gz

echo "Building extensions package"
cd -
cd python-packages/extensions
rm -f dist/*gz
python3 -m build

echo "Installing extensions package"
pip3 install dist/omigo_ext-*.tar.gz

echo "Building hydra package"
cd -
cd python-packages/hydra
rm -f dist/*gz
python3 -m build

echo "Installing hydra package"
pip3 install dist/omigo_hydra-*.tar.gz

