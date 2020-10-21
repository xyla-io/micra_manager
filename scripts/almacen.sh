#!/bin/bash

echo "Starting Almacen worker run"
cd "$1"
echo "Micra started Almacen worker run: ${@:2}"
source ./venv/bin/activate
python almacen.py "${@:2}"
ALMACENCODE=$?
deactivate
if [ $ALMACENCODE -eq 0 ]; then
  echo "Micra finished Almacen worker run: ${@:2}"
else
  echo "Micra error on Almacen worker run: ${@:2} code: ${$?}"
fi
exit $ALMACENCODE