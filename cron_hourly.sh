#!/bin/bash

# 0 0 * * * ~/politylink/politylink-tools/cron_hourly.sh &> ~/politylink/politylink-tools/log/cron_hourly.log

set -ue

cd ~/politylink/politylink-tools
logrotate logrotate.conf -s /tmp/logrotate.state
poetry run python cron.py -m hourly
