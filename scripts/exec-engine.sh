#!/bin/sh
BASE_DIR=/var/www/trade-exec-engine
cd "$BASE_DIR"

TARGET_TIME="21:35"

if [ -z "$1" ]; then
  echo "Error: missing Account parameter."
  exit 1
fi

rm logs/engine.log 2> /dev/null
. .venv/bin/activate
today="$(date -I)"
now="$(date +"%Y-%m-%d_%H-%M-%S")"
export ACCOUNT=${1}
echo "Account:${ACCOUNT}"
export GENERATED_PATH="$BASE_DIR"/generated
export RESOURCE_PATH="$BASE_DIR"/resources/config
export LOG_PATH="$BASE_DIR"/logs
sleep 1
while true;
do
    current_time=$(date +"%H:%M")
    if [[ "$current_time" > "$TARGET_TIME" ]]; then
        echo "Stopping the loop at 15:15 hours."
        break
    fi
    python run-engine.py "${ACCOUNT}" 1> logs/exec-engine-"${ACCOUNT}".log 2> logs/exec-engine-"${ACCOUNT}".err
done
mkdir logs/archive/${today} 2> /dev/null
gzip logs/engine-"${ACCOUNT}".log
mv logs/engine-"${ACCOUNT}".log.gz logs/archive/${today}/engine-"${ACCOUNT}".log.${now}.gz
mv logs/exec-engine-"${ACCOUNT}".* logs/archive/${today}/
