#!/bin/zsh

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

mkdir logs
mkdir tv-data
mkdir tv-data/low-tf-data
mkdir tv-data/base-data
read -p "Please Enter Dropbox Path: E.g. /Users/user/Dropbox: " -r dropbox
ln -sf "$dropbox"/Trader .

echo "Done!"