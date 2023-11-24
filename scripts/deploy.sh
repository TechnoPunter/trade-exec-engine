#!/usr/bin/env sh
cd /var/www/trade-exec-engine
. .venv/bin/activate
pip uninstall -y TechnoPunter-Commons
pip install -r requirements.txt