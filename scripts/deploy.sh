#!/usr/bin/env sh
cd /var/www/trade-exec-engine
git pull
. .venv/bin/activate
pip uninstall -y TechnoPunter-Commons
pip install -r requirements.txt