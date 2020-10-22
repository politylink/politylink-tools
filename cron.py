import argparse
import logging
import subprocess
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path

LOGGER = logging.getLogger(__name__)

POLITYLINK_ROOT = Path.home() / 'politylink'
CRAWLER_ROOT = POLITYLINK_ROOT / 'politylink-crawler'
GATSBY_ROOT = POLITYLINK_ROOT / 'politylink-gatsby'
TOOLS_ROOT = POLITYLINK_ROOT / 'politylink-tools'
LOG_ROOT = TOOLS_ROOT / 'log'

TODAY = datetime.now().date()
YESTERDAY = TODAY - timedelta(1)
SEVEN_DAYS_AGO = TODAY - timedelta(7)
DATE_FORMAT = '%Y-%m-%d'


class BashTask:
    def __init__(self, cmd, cwd=None, log_fp=None):
        self.cmd = cmd
        self.cwd = cwd if cwd else '.'
        self.log_fp = LOG_ROOT / log_fp if log_fp else '/dev/null'

    def __repr__(self):
        return f'<BashTask {self.cmd}>'

    def run(self):
        with open(self.log_fp, 'w') as f:
            return subprocess.run(self.cmd, shell=True, cwd=self.cwd, stdout=f, stderr=f, encoding='utf-8')


DAILY_TASKS = [
    BashTask('poetry run scrapy crawl shugiin', CRAWLER_ROOT, 'crawl_shugiin.log'),
    BashTask('poetry run scrapy crawl sangiin', CRAWLER_ROOT, 'crawl_sangiin.log'),
    BashTask('poetry run scrapy crawl shugiin_committee', CRAWLER_ROOT, 'crawl_shugiin_committee.log'),
    BashTask('poetry run scrapy crawl sangiin_committee', CRAWLER_ROOT, 'crawl_sangiin_committee.log'),
    BashTask('poetry run scrapy crawl minutes -a start_date={} -a end_date={} -a speech=false'.format(
        SEVEN_DAYS_AGO.strftime(DATE_FORMAT), TODAY.strftime(DATE_FORMAT)),
        CRAWLER_ROOT, 'crawl_minutes.log'),
]

HOURLY_TASKS = [
    BashTask('poetry run scrapy crawl reuters -a limit=50', CRAWLER_ROOT, 'crawl_reuters.log'),
    BashTask('poetry run scrapy crawl reuters_kyodo -a limit=50', CRAWLER_ROOT, 'crawl_reuters_kyodo.log'),
    BashTask('poetry run scrapy crawl nikkei -a limit=50', CRAWLER_ROOT, 'crawl_nikkei.log'),
    BashTask('poetry run scrapy crawl mainichi -a limit=50', CRAWLER_ROOT, 'crawl_mainichi.log'),
    BashTask('poetry run python news.py --start_date {} --end_date {}'.format(
        YESTERDAY.strftime(DATE_FORMAT), TODAY.strftime(DATE_FORMAT)),
        TOOLS_ROOT, 'process_news.log'),
    BashTask('poetry run python timeline.py --start_date {} --end_date {}'.format(
        YESTERDAY.strftime(DATE_FORMAT), TODAY.strftime(DATE_FORMAT)),
        TOOLS_ROOT, 'process_timeline.log'),
    BashTask('gatsby clean && gatsby build', GATSBY_ROOT, 'gatsby.log'),
]


class Mode(Enum):
    DAILY = 'daily'
    HOURLY = 'hourly'

    def __str__(self):
        return self.value

    def tasks(self):
        if self == Mode.DAILY:
            return DAILY_TASKS
        elif self == Mode.HOURLY:
            return HOURLY_TASKS


def main(mode):
    for task in mode.tasks():
        LOGGER.info(f'run {task.cmd}')
        try:
            result = task.run()
            if result.returncode != 0:
                LOGGER.warning(
                    f'received non-zero returncode={result.returncode}. check {task.log_fp} for the details.')
        except Exception:
            LOGGER.exception(f'failed to run {task.cmd}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='cron用のタスクを管理する')
    parser.add_argument('-m', '--mode', type=Mode, choices=list(Mode), required=True)
    parser.add_argument('-v', '--verbose', action='store_true')
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO)
    main(args.mode)
