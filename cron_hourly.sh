#!/bin/bash --login

# crontab -e
# 0 * * * * ~/politylink/politylink-tools/cron_hourly.sh &> ~/politylink/politylink-tools/log/hourly/cron.log

set -ue

cd ~/politylink/politylink-tools
logrotate ./data/logrotate_hourly.conf -s /tmp/logrotate.state
poetry run python cron.py -m hourly
