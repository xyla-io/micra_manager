#!/bin/bash

set -e
set -x

source local_configure
PYTHONCOMMAND=${PYTHONCOMMAND:-python3}
PLATFORM=${PLATFORM:-osx}
DEVELOPMENTPACKAGES="micra_store moda"

if [ "${PLATFORM}" != 'docker' ]; then
  git submodule update --init
fi

rm -rf .venv
$PYTHONCOMMAND -m venv .venv
source .venv/bin/activate

pip install --upgrade pip
pip install -r requirements.txt

if [ "$DEVELOPMENTPACKAGES" != "" ]; then
  cd development_packages
  for DEVPKG in $DEVELOPMENTPACKAGES
  do
    cd $DEVPKG
    if [ -f requirements.txt ]; then
      pip install -r requirements.txt
    fi
    python setup.py develop
    cd ..
  done
  cd ..
fi

deactivate