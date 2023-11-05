#!/bin/sh
cd /var/www/trade-exec-engine

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
sleep 1
python run-engine.py "${ACCOUNT}" 1> logs/exec-engine-"${ACCOUNT}".log 2> logs/exec-engine-"${ACCOUNT}".err
mkdir logs/archive/${today} 2> /dev/null
gzip logs/engine-"${ACCOUNT}".log
mv logs/engine-"${ACCOUNT}".log.gz logs/archive/${today}/engine-"${ACCOUNT}".log.${now}.gz
mv logs/exec-engine-"${ACCOUNT}".* logs/archive/${today}/
