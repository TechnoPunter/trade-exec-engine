#!/bin/sh
cd /var/www/trade-exec-engine
rm logs/socket.log 2> /dev/null
. .venv/bin/activate
today="$(date -I)"
now="$(date +"%Y-%m-%d_%H-%M-%S")"
export ACCOUNT=${1}
echo "Account:${ACCOUNT}"
sleep 1
python run-socket.py "${ACCOUNT}" 1> logs/exec-socket-"${ACCOUNT}".log 2> logs/exec-socket-"${ACCOUNT}".err
mkdir logs/archive/${today} 2> /dev/null
gzip logs/socket-"${ACCOUNT}".log
mv logs/socket-"${ACCOUNT}".log.gz logs/archive/${today}/socket-"${ACCOUNT}".log.${now}.gz
mv logs/exec-socket-"${ACCOUNT}".* logs/archive/${today}/
