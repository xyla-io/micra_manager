#!/bin/bash
cd "$( dirname "$0" )"
SCRIPT_DIR=$(pwd)
source ${SCRIPT_DIR}/.venv/bin/activate
python micra_manager.py "$@"
