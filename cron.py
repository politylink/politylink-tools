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
TOMORROW = TODAY + timedelta(1)
DAY_AFTER_TOMORROW = TODAY + timedelta(2)
SEVEN_DAYS_AGO = TODAY - timedelta(7)
ONE_MONTH_AGO = TODAY - timedelta(30)
DATE_FORMAT = '%Y-%m-%d'

LOG_DATE_FORMAT = "%Y-%m-%d %I:%M:%S"
LOG_FORMAT = '%(asctime)s [%(name)s] %(levelname)s: %(message)s'


class BashTask:
    def __init__(self, cmd, cwd=None, log_fp=None):
        self.cmd = cmd
        self.cwd = cwd if cwd else '.'
        self.log_fp = str(log_fp) if log_fp else '/dev/null'

    def __repr__(self):
        return f'<BashTask {self.cmd}>'

    def run(self):
        with open(self.log_fp, 'w') as f:
            return subprocess.run(self.cmd, shell=True, cwd=self.cwd, stdout=f, stderr=f, encoding='utf-8')


DAILY_LOG_ROOT = LOG_ROOT / 'daily'
DAILY_TASKS = [
    BashTask('poetry run scrapy crawl shugiin',
             CRAWLER_ROOT, DAILY_LOG_ROOT / 'crawl_shugiin.log'),
    BashTask('poetry run scrapy crawl sangiin -a diet=203',
             CRAWLER_ROOT, DAILY_LOG_ROOT / 'crawl_sangiin.log'),
    BashTask('poetry run scrapy crawl shugiin_committee',
             CRAWLER_ROOT, DAILY_LOG_ROOT / 'crawl_shugiin_committee.log'),
    BashTask('poetry run scrapy crawl sangiin_committee',
             CRAWLER_ROOT, DAILY_LOG_ROOT / 'crawl_sangiin_committee.log'),
    BashTask('poetry run scrapy crawl shugiin_minutes',
             CRAWLER_ROOT, DAILY_LOG_ROOT / 'crawl_shugiin_minutes.log'),
    BashTask('poetry run scrapy crawl sangiin_minutes',
             CRAWLER_ROOT, DAILY_LOG_ROOT / 'crawl_sangiin_minutes.log'),
    BashTask('poetry run scrapy crawl minutes -a start_date={} -a end_date={} -a speech=false'.format(
        ONE_MONTH_AGO.strftime(DATE_FORMAT), TOMORROW.strftime(DATE_FORMAT)),
        CRAWLER_ROOT, DAILY_LOG_ROOT / 'crawl_minutes.log'),
    BashTask('poetry run python news.py --start_date {} --end_date {}'.format(
        SEVEN_DAYS_AGO.strftime(DATE_FORMAT), TOMORROW.strftime(DATE_FORMAT)),
        TOOLS_ROOT, DAILY_LOG_ROOT / 'process_news.log'),
    BashTask('poetry run python timeline.py --start_date {} --end_date {}'.format(
        ONE_MONTH_AGO.strftime(DATE_FORMAT), DAY_AFTER_TOMORROW.strftime(DATE_FORMAT)),
        TOOLS_ROOT, DAILY_LOG_ROOT / 'process_timeline.log'),
]

HOURLY_LOG_ROOT = LOG_ROOT / 'hourly'
HOURLY_TASKS = [
    BashTask('poetry run scrapy crawl reuters -a limit=50',
             CRAWLER_ROOT, HOURLY_LOG_ROOT / 'crawl_reuters.log'),
    BashTask('poetry run scrapy crawl nikkei -a limit=50',
             CRAWLER_ROOT, HOURLY_LOG_ROOT / 'crawl_nikkei.log'),
    BashTask('poetry run scrapy crawl mainichi -a limit=50',
             CRAWLER_ROOT, HOURLY_LOG_ROOT / 'crawl_mainichi.log'),
    BashTask('poetry run scrapy crawl shugiin_tv -a start_date={} -a end_date={}'.format(
        TODAY.strftime(DATE_FORMAT), TOMORROW.strftime(DATE_FORMAT)),
        CRAWLER_ROOT, HOURLY_LOG_ROOT / 'crawl_shugiin_tv.log'),
    BashTask('poetry run scrapy crawl sangiin_tv',
             CRAWLER_ROOT, HOURLY_LOG_ROOT / 'crawl_sangiin_tv.log'),
    BashTask('poetry run python news.py --start_date {} --end_date {}'.format(
        TODAY.strftime(DATE_FORMAT), TOMORROW.strftime(DATE_FORMAT)),
        TOOLS_ROOT, HOURLY_LOG_ROOT / 'process_news.log'),
    BashTask('poetry run python timeline.py --start_date {} --end_date {}'.format(
        TODAY.strftime(DATE_FORMAT), DAY_AFTER_TOMORROW.strftime(DATE_FORMAT)),
        TOOLS_ROOT, HOURLY_LOG_ROOT / 'process_timeline.log'),
    BashTask('gatsby build',
             GATSBY_ROOT, HOURLY_LOG_ROOT / 'gatsby.log'),
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
        LOGGER.info(f'{task.cmd} @ {task.cwd}')
        try:
            result = task.run()
            if result.returncode != 0:
                LOGGER.warning(result)
                LOGGER.warning(
                    f'received non-zero returncode={result.returncode}. check {task.log_fp} for the details.')
        except Exception:
            LOGGER.exception(f'failed to run {task.cmd}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='cron用のタスクを管理する')
    parser.add_argument('-m', '--mode', type=Mode, choices=list(Mode), required=True)
    parser.add_argument('-v', '--verbose', action='store_true')
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        datefmt=LOG_DATE_FORMAT, format=LOG_FORMAT)
    main(args.mode)
