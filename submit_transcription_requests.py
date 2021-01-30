import argparse
import glob
import logging
import re
from datetime import datetime

import requests

from cron import BashTask, TOOLS_ROOT, LOG_ROOT
from politylink.helpers import MinutesFinder
from utils import date_type

LOGGER = logging.getLogger(__name__)


def get_video_url(urls):
    for url in urls:
        if url.title == '審議中継':
            return url.url
    raise ValueError(f'video url not found')


def get_m3u8_url(video_url):
    # ToDO: check if meeting is finished
    try:
        response = requests.get(video_url)
        pattern = 'https?://.*playlist.m3u8'
        mm3u8_url = re.search(pattern, response.text).group()
        return mm3u8_url.replace('http://', 'https://')
    except Exception:
        raise ValueError('m3u8 url not found')


def main():
    minutes_finder = MinutesFinder(url='https://graphql.politylink.jp')
    minutes_list = minutes_finder.find(text='', dt=args.date)
    LOGGER.info(f'found {len(minutes_list)} minutes on {args.date.strftime("%Y-%m-%d")}')

    tasks = []
    for minutes in minutes_list:
        job_name = minutes.id.split(':')[-1]
        job_files = glob.glob(f'./voice/{job_name}*')
        if not args.overwrite and job_files:
            LOGGER.info(f'{job_name} is already processed, skipping: {job_files}')
            continue
        try:
            video_url = get_video_url(minutes.urls)
            m3u8_url = get_m3u8_url(video_url)
            task = BashTask(f'bash transcribe_voice.sh {job_name} {m3u8_url}',
                            TOOLS_ROOT, LOG_ROOT / 'voice' / f'{job_name}.log')
            tasks.append(task)
            LOGGER.info(f'created task for {job_name}')
        except Exception:
            LOGGER.exception(f'failed to create task for {job_name}')
    LOGGER.info(f'created total {len(tasks)} tasks')

    for task in tasks:
        LOGGER.info(f'run: {task.cmd}')
        LOGGER.info(f'logs will be saved in {task.log_fp}')
        task.run(wait=False)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='音声ファイルをGCPの文字起こしAPIに投げる')
    parser.add_argument('-d', '--date', help='文字起こしする日付（yyyy-mm-dd）', type=date_type, default=datetime.now())
    parser.add_argument('-f', '--file', default='./voice/submit')
    parser.add_argument('-v', '--verbose', action='store_true')
    parser.add_argument('-o', '--overwrite', help='既に実行済ファイルがあっても実行する', action='store_true')
    args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO)
    logging.getLogger('sgqlc').setLevel(logging.INFO)
    main()
