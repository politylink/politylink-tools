import argparse
import dataclasses
import glob
import json
import logging
import os
import re
from typing import List

import boto3
import pandas as pd
from jinja2 import Environment, FileSystemLoader

from politylink.graphql.client import GraphQLClient
from politylink.graphql.schema import Url, Minutes
from politylink.idgen import idgen

LOGGER = logging.getLogger(__name__)
VIDEO_DIFF_THRESH = 0.5
VOICE_DIFF_THRESH = 3

gql_client = GraphQLClient(url='https://graphql.politylink.jp')
s3_client = boto3.client('s3')


@dataclasses.dataclass
class VoiceSegment:
    transcript: str
    start_time: float
    end_time: float
    is_first: bool = False


def load_voice_segments(json_fp):
    """
    load voice segments from GCP transcription result (JSON)
    ref: fetch_transcription_results.py
    """

    def parse_time_str(time_str):
        return float(time_str[:-1])  # remove last "s"

    with open(json_fp, 'r') as f:
        data = json.load(f)

    segments = []
    for result in data['response']['results'][:-1]:
        result = result['alternatives'][0]
        segment = VoiceSegment(
            result['transcript'],
            parse_time_str(result['words'][0]['startTime']),
            parse_time_str(result['words'][-1]['endTime'])
        )
        segments.append(segment)
    return segments


def load_video_breaks(diff_fp, thresh_diff):
    """
    load video break secs from video diff file (CSV)
    ref: diff_video.py
    """

    if not os.path.exists(diff_fp):
        LOGGER.warning(f'video diff file does not exist: {diff_fp}')
        return list()

    diff_df = pd.read_csv(diff_fp)
    diff_df = diff_df[diff_df['diff'] > thresh_diff]
    return list(diff_df['sec'])


def set_is_first_by_time(segments: List[VoiceSegment], thresh_sec):
    """
    set is_first flag to VoiceSegment by time threshold
    inputs need to be sorted in ascending order of time
    """

    for i in range(1, len(segments)):
        if segments[i].start_time - segments[i - 1].end_time > thresh_sec:
            segments[i].is_first = True


def set_is_first_by_video(segments: List[VoiceSegment], video_breaks: List[int]):
    """
    set is_first flag to VoiceSegment by video camera switch seconds
    inputs need to be sorted in ascending order of time
    """

    i, j = 0, 0
    while i < len(segments) and j < len(video_breaks):
        seg = segments[i]
        bre = video_breaks[j]
        if seg.end_time < bre:
            i += 1
        elif bre < seg.start_time:
            segments[i].is_first = True
            j += 1
        elif seg.start_time <= bre <= seg.end_time:
            if bre - seg.start_time < seg.end_time - bre:
                segments[i].is_first = True
            else:
                if i + 1 < len(segments):
                    segments[i + 1].is_first = True
            j += 1


def insert_punctuation(text):
    suffixes = ['ました', 'します', 'きます', 'います', 'ります']
    for suffix in suffixes:
        pattern = r'{}(。)?'.format(suffix)
        repl = '{}。'.format(suffix)
        text = re.sub(pattern, repl, text)
    if text[-1] != '。':
        text += '。'
    return text


def build_html(voice_segments: List[VoiceSegment], minutes: Minutes):
    buffer = ''
    transcripts = []
    for segment in voice_segments:
        if segment.is_first and buffer:
            transcripts.append(buffer)
            buffer = ''
        buffer += insert_punctuation(segment.transcript)
    if buffer:
        transcripts.append(buffer)

    template = Environment(loader=FileSystemLoader('./data', encoding='utf8')) \
        .get_template('transcription_template.html')
    date = minutes.start_date_time
    date_str = f'{date.year:02}-{date.month:02}-{date.day:02}'
    html = template.render({
        'minutes': minutes.name,
        'date': date_str,
        'url': 'https://politylink.jp/minutes/{}'.format(minutes.id.split(':')[-1]),
        'transcripts': transcripts
    })
    return html


def build_gql_url(s3_url):
    url = Url({
        'url': s3_url,
        'title': '自動文字起こし',
        'domain': 'politylink.jp'
    })
    url.id = idgen(url)
    return url


def process(job_id, time_thresh, diff_thresh, publish):
    LOGGER.info(f'process {job_id}')
    minutes = gql_client.get(f'Minutes:{job_id}')
    json_fp = f'./voice/{job_id}.json'
    diff_fp = f'./voice/{job_id}.diff'
    html_fp = f'./voice/{job_id}.html'
    s3_json_fp = f'minutes/{job_id}.json'
    s3_html_fp = f'minutes/{job_id}.html'
    s3_html_url = f'https://text.politylink.jp/{s3_html_fp}'

    voice_segments = load_voice_segments(json_fp)
    LOGGER.info(f'loaded {len(voice_segments)} voice segments from {json_fp}')
    video_breaks = load_video_breaks(diff_fp, diff_thresh)
    LOGGER.info(f'loaded {len(video_breaks)} video breaks from {diff_fp}')

    voice_segments[0].is_first = True
    set_is_first_by_time(voice_segments, time_thresh)
    set_is_first_by_video(voice_segments, video_breaks)
    LOGGER.info(f'set is_first flag to {sum(map(lambda x: x.is_first, voice_segments))} voice segments')

    html = build_html(voice_segments, minutes)
    with open(html_fp, 'w', encoding="utf-8") as f:
        f.write(html)
    LOGGER.info(f'saved HTML in {html_fp}')

    if publish:
        s3_client.upload_file(json_fp, 'politylink-text', s3_json_fp, ExtraArgs={"ContentType": "application/json"})
        s3_client.upload_file(html_fp, 'politylink-text', s3_html_fp, ExtraArgs={"ContentType": "text/html"})
        gql_url = build_gql_url(s3_html_url)
        gql_client.merge(gql_url)
        gql_client.link(gql_url.id, minutes.id)
        LOGGER.info(f'published HTML to S3: {s3_html_url}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='文字起こしの結果をHTMLに加工する')
    parser.add_argument('-i', '--id', help='文字起こしのJob ID（PolityLinkのMinutes IDのBody）。指定しない場合はHTMLが存在しない全てのJSONを処理する')
    parser.add_argument('-tt', '--time_thresh', help='この閾値（sec）より長く音声が途切れたら改行する', default=3)
    parser.add_argument('-dt', '--diff_thresh', help='この閾値（rate）より大きく動画のフレームが変化したら改行する', default=0.5)
    parser.add_argument('-p', '--publish', help='S3にHTMLをアップロードする', action='store_true')
    parser.add_argument('-v', '--verbose', action='store_true')
    args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO)
    logging.getLogger('sgqlc').setLevel(logging.INFO)

    if args.id:
        # process specified ID
        ids = [args.id]
    else:
        # process all IDs without HTML
        json_ids = set(map(lambda fp: fp.split('/')[-1].split('.')[0], glob.glob('./voice/*.json')))
        html_ids = set(map(lambda fp: fp.split('/')[-1].split('.')[0], glob.glob('./voice/*.html')))
        ids = list(json_ids - html_ids)

    LOGGER.info(f'found {len(ids)} ids to process: {ids}')
    for id_ in ids:
        try:
            process(id_, args.time_thresh, args.diff_thresh, args.publish)
        except Exception:
            LOGGER.exception(f'failed to process {id_}')
