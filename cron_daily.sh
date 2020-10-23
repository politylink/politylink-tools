#!/bin/bash --login

# crontab -e
# 0 0 * * * ~/politylink/politylink-tools/cron_daily.sh &> ~/politylink/politylink-tools/log/daily/cron.log

set -ue

cd ~/politylink/politylink-tools
logrotate ./data/logrotate_daily.conf -s /tmp/logrotate.state
poetry run python cron.py -m daily
