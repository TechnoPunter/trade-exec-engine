#!/bin/sh
BASE_DIR=/var/www/trade-exec-engine
cd "$BASE_DIR"

rm logs/cob.log 2> /dev/null
. .venv/bin/activate
today="$(date -I)"
now="$(date +"%Y-%m-%d_%H-%M-%S")"

export GENERATED_PATH="$BASE_DIR"/generated
export RESOURCE_PATH="$BASE_DIR"/resources/config
export LOG_PATH="$BASE_DIR"/logs

python run-cob.py 1> logs/exec-cob.log 2> logs/exec-cob.err

mkdir logs/archive/${today} 2> /dev/null
gzip logs/cob.log
mv logs/cob.log.gz logs/archive/${today}/cob.log.${now}.gz
mv logs/exec-cob.* logs/archive/${today}/
