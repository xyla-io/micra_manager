SERVICE_INSTALL_DIR="/etc/systemd/system"
TEMP_FILE=".tmp.service"

IFS="/" read -ra CWD <<< "$(pwd)"
IFS=" "

# Defaults
DEFAULT_SERVICE_SLUG="micra_manager"
DEFAULT_DESCRIPTION="Micra Manager"
DEFAULT_START_COMMAND="/bin/bash micra_manager.sh"
DEFAULT_USER="${CWD[2]}"

# Prompts
PROMPT_SERVICE_SLUG="Enter slug for the service daemon [$DEFAULT_SERVICE_SLUG]: "
PROMPT_DESCRIPTION="Enter human-readable service description [$DEFAULT_DESCRIPTION]: "
PROMPT_START_COMMAND="Enter application start command [$DEFAULT_START_COMMAND]: "
PROMPT_USER="Enter server user [$DEFAULT_USER]: "
PROMPT_GROUP="Enter server group [$DEFAULT_USER]: "

[ -f $TEMP_FILE ] && rm $TEMP_FILE

read -e -p "$PROMPT_SERVICE_SLUG" SERVICE_SLUG
[[ "$SERVICE_SLUG" == "" ]] && SERVICE_SLUG=$DEFAULT_SERVICE_SLUG
while [[ $SERVICE_SLUG = *" "* ]]; do
  echo "Service slug cannot contain spaces."
  read -e -p "$PROMPT_SERVICE_SLUG" SERVICE_SLUG
done

# [Unit]
echo "[Unit]" >> $TEMP_FILE
read -e -p "$PROMPT_DESCRIPTION" DESCRIPTION
[[ "$DESCRIPTION" == "" ]] && DESCRIPTION="$DEFAULT_DESCRIPTION"
echo "Description=${DESCRIPTION}" >> $TEMP_FILE
echo "After=network.target" >> $TEMP_FILE

echo >> $TEMP_FILE

# [Service]
echo "[Service]" >> $TEMP_FILE
echo "WorkingDirectory=`pwd`" >> $TEMP_FILE
echo "Restart=always" >> $TEMP_FILE
read -e -p "$PROMPT_START_COMMAND" START_COMMAND
[[ "$START_COMMAND" == "" ]] && START_COMMAND="$DEFAULT_START_COMMAND"
echo "ExecStart=${START_COMMAND}" >> $TEMP_FILE
read -e -p "$PROMPT_USER" USER
[[ "$USER" == "" ]] && USER="$DEFAULT_USER"
echo "User=${USER}" >> $TEMP_FILE
read -e -p "$PROMPT_GROUP" GROUP
[[ "$GROUP" == "" ]] && GROUP="$DEFAULT_USER"
echo "Group=${GROUP}" >> $TEMP_FILE

echo >> $TEMP_FILE

# [Install]
echo "[Install]" >> $TEMP_FILE
echo "WantedBy=multi-user.target" >> $TEMP_FILE

echo
echo "Here is the file to be installed to $SERVICE_INSTALL_DIR/$SERVICE_SLUG.service :"
echo "------------------------------------------------------------------------"
cat $TEMP_FILE
echo "------------------------------------------------------------------------"
echo
read -e -p "Would you like to install this service? [Y/n]: " CONFIRM
if [[ $CONFIRM == "y" || $CONFIRM == "Y" || $CONFIRM == "" ]]; then
  if systemctl status $SERVICE_SLUG > /dev/null; then
    echo "A service with the name $SERVICE_SLUG.service already exists."
    read -e -p "Do you want to stop, disable, and overwrite the existing service? [y/N]: " YN
    if [[ $YN == "y" || $YN == "Y" ]]; then
      systemctl stop $SERVICE_SLUG
      systemctl disable $SERVICE_SLUG
    else
      echo "Service will not be overwritten."
      echo "Newly generated file can be inspected at `pwd`/$TEMP_FILE"
      exit 0
    fi
  fi
  cp $TEMP_FILE $SERVICE_INSTALL_DIR/$SERVICE_SLUG.service
  echo "Service has been installed to $SERVICE_INSTALL_DIR/$SERVICE_SLUG.service"
  read -e -p "Do you want to enable the service on server reboot? [Y/n]: " YN
  if [[ $YN == "y" || $YN == "Y" || $YN == "" ]]; then
    systemctl enable $SERVICE_SLUG
    echo "Service enabled."
  fi
  read -e -p "Do you want to start the service now? [Y/n]: " YN
  if [[ $YN == "y" || $YN == "Y" || $YN == "" ]]; then
    systemctl start $SERVICE_SLUG
    echo "Service started."
  fi
  rm $TEMP_FILE
else
  echo "Nothing more to do."
  echo "Rejected file can be inspected at `pwd`/$TEMP_FILE"
fi

