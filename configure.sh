#!/bin/bash

set -e

INTERACT=false
INPUTFILE=""

while getopts "ic:" OPT; do
    case "$OPT" in
    i)  INTERACT=true
        ;;
    c)  INPUTFILE=$OPTARG
        ;;
    esac
done

LF=$'\n'

JSONPYTHON=$(which python3.7 | tr -d '\n')
if [ -z "${JSONPYTHON}" ]; then
  JSONPYTHON=$(which python3 | tr -d '\n')
fi
if [ -z "${JSONPYTHON}" ]; then
  JSONPYTHON=$(which python | tr -d '\n')
fi

if [ "${INTERACT}" = true ]; then
  read -p "Please enter the python command to parse JSON [${JSONPYTHON}]: " JSONPYTHONINPUT
  JSONPYTHON=${JSONPYTHONINPUT:-"${JSONPYTHON}"}
fi

LOCALFILE=""
if [ -f "local_configure.json" ]; then
  LOCALFILE="local_configure.json"
fi

CONFIG=$($JSONPYTHON -c "import sys, json${LF}d = {}${LF}for p in sys.argv[1:]:${LF}  if not p: continue${LF}  with open(p, 'r') as f:${LF}    c = json.load(f)${LF}  d = {**d, **c}${LF}print('\n'.join([f'{k} {json.dumps(d[k], sort_keys=True)}' for k in sorted(d.keys())]))" "configure.json" "${LOCALFILE}" "${INPUTFILE}")

LOCALSCRIPTCONFIG=""
LOCALCONFIG="{"
while read -r LINE <&3; do
  PARAMNAME=$(echo "${LINE}" | awk '{print $1}')
  PARAMVALUE=$(echo "${LINE}" | awk '{$1=""; print $0}')
  PARAMVALUE="${PARAMVALUE:1}"
  if [ "${INTERACT}" = true ]; then
    read -p "${PARAMNAME} [${PARAMVALUE}]: " ENTEREDVALUE
    ENTEREDVALUE=${ENTEREDVALUE:-"${PARAMVALUE}"}
  else
    ENTEREDVALUE="${PARAMVALUE}"
  fi
  LOCALCONFIG="${LOCALCONFIG}${LF}  \"${PARAMNAME}\": ${ENTEREDVALUE},"
  if [ "${PARAMNAME}" == "python_command" ]; then
    LOCALSCRIPTCONFIG="${LOCALSCRIPTCONFIG}${LF}PYTHONCOMMAND=${ENTEREDVALUE}${LF}"
  fi
  if [ "${PARAMNAME}" == "platform" ]; then
    LOCALSCRIPTCONFIG="${LOCALSCRIPTCONFIG}PLATFORM=${ENTEREDVALUE}${LF}"
  fi
done 3<<< "${CONFIG}"
LOCALCONFIG="${LOCALCONFIG::${#LOCALCONFIG}-1}${LF}}"
echo "Configuration:"
echo "${LOCALCONFIG}"
if [ "${INTERACT}" = true ]; then
  read -p "Write this configuration to local_configure.json [Y/n]: " CONFIRM
  CONFIRM=${CONFIRM:-y}
else
  CONFIRM="y"
fi
if [ "${CONFIRM:0:1}" == "y" ] || [ "${CONFIRM:0:1}" == "Y" ] ; then
  echo "${LOCALCONFIG}" > local_configure.json
  echo "${LOCALSCRIPTCONFIG}" > local_configure
fi