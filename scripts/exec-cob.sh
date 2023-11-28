#!/bin/sh
BASE_DIR=/var/www/trade-exec-engine
cd "$BASE_DIR"

if [ -z "$1" ]; then
  echo "Error: missing Account parameter."
  exit 1
fi

rm logs/cob.log 2> /dev/null
. .venv/bin/activate
today="$(date -I)"
now="$(date +"%Y-%m-%d_%H-%M-%S")"
export ACCOUNT=${1}
echo "Account:${ACCOUNT}"
export GENERATED_PATH="$BASE_DIR"/generated
export RESOURCE_PATH="$BASE_DIR"/resources/config
export LOG_PATH="$BASE_DIR"/logs
python run-cob.py "${ACCOUNT}" 1> logs/exec-cob-"${ACCOUNT}".log 2> logs/exec-cob-"${ACCOUNT}".err
mkdir logs/archive/${today} 2> /dev/null
gzip logs/cob-"${ACCOUNT}".log
mv logs/cob-"${ACCOUNT}".log.gz logs/archive/${today}/cob-"${ACCOUNT}".log.${now}.gz
mv logs/exec-cob-"${ACCOUNT}".* logs/archive/${today}/
