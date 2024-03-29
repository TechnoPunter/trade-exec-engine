#!/bin/zsh

# Need to reach Project Root
MODULE_NAME=trade-exec-engine
CURR_PATH=$(pwd)

if [[ $CURR_PATH == *$MODULE_NAME* ]]; then
  sleep 1
else
  echo "Need to run from $MODULE_NAME location"
  exit 1
fi

if [[ $CURR_PATH == *scripts* ]]; then
  CURR_PATH="${CURR_PATH%%scripts*}"
fi

cd "$CURR_PATH" || exit 1


# Install & create venv
sudo apt-get install python-virtualenv
virtualenv --python=/usr/bin/python3.10 .venv

source .venv/bin/activate
pip install -r requirements.txt

TRAINER_GEN_PATH="$CURR_PATH"/../model-trainer/generated
ln -sf "$TRAINER_GEN_PATH" .

mkdir logs
mkdir tv-data
mkdir tv-data/low-tf-data
mkdir tv-data/base-data
read -p "Please Enter Dropbox Path: E.g. /Users/user/Dropbox: " -r dropbox

if [[ -d $dropbox ]]; then
  sleep 1
else
  echo "Please check $dropbox directory!"
  exit 1
fi

ln -sf "$dropbox"/Trader .
ln -sf "$dropbox"/Trader/secret .
cd logs || exit 1
ln -sf "$dropbox"/Trader/trade-exec-engine-V2/logs/archive .


FILE=resources/config/secrets-local.yaml
if [[ -f $FILE ]]; then
  echo "Done!"
else
  echo "Please create / check secrets-local.yaml file in resources/config directory!"
fi

