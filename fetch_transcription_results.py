import argparse
import json
import logging
import os

import boto3
import requests
from jinja2 import Environment, FileSystemLoader

from politylink.graphql.client import GraphQLClient
from politylink.graphql.schema import Url
from politylink.idgen import idgen
from transcribe_voice import insert_punctuation

LOGGER = logging.getLogger(__name__)


class SpeechRestClient:
    """
    https://cloud.google.com/speech-to-text/docs/reference/rest
    """

    def __init__(self):
        self.api_key = os.environ['GCP_API_KEY']

    def list_(self):
        """
        return currently available operation ids
        """

        url = f'https://speech.googleapis.com/v1/operations?key={self.api_key}'
        response = requests.get(url)
        data = response.json()
        return list(map(lambda x: x['name'], data['operations']))

    def get(self, id_):
        """
        return finished operation result
        """

        url = f'https://speech.googleapis.com/v1/operations/{id_}?key={self.api_key}'
        response = requests.get(url)
        data = response.json()

        if ('done' not in data) or (not data['done']):
            raise ValueError('operation is not finished')

        if 'error' in data:
            raise ValueError(f'operation failed: {data["error"]}')

        data['metadata']['job_name'] = data['metadata']['uri'].split('/')[-1].replace('.mp3', '')
        return data


def parse_time_str(time_str):
    return float(time_str[:-1])  # remove last "s"


def format_transcripts(data, time_thresh=1):
    transcripts = []
    last_end_time = 0.0
    buffer = ''

    results = data['response']['results']
    for result in results[:-1]:  # skip last summary
        result = result['alternatives'][0]  # get best result
        raw_text = result['transcript']
        formatted_text = insert_punctuation(raw_text)
        start_time = parse_time_str(result['words'][0]['startTime'])
        end_time = parse_time_str(result['words'][-1]['endTime'])

        if start_time - last_end_time >= time_thresh:  # likely speaker has changed
            if buffer:  # flush buffer
                transcripts.append(buffer)
                buffer = ''

        buffer += formatted_text
        last_end_time = end_time

    if buffer:
        transcripts.append(buffer)
    return transcripts


def build_html(template, data, minutes):
    date = minutes.start_date_time
    transcripts = format_transcripts(data, time_thresh=1)
    html = template.render({
        'minutes': minutes.name,
        'date': f'{date.year}-{date.month}-{date.day}',
        'url': 'https://politylink.jp/minutes/{}'.format(minutes.id.split(':')[-1]),
        'transcripts': transcripts
    })
    return html


def build_url(s3_url):
    url = Url({
        'url': s3_url,
        'title': '自動文字起こし',
        'domain': 'politylink.jp'
    })
    url.id = idgen(url)
    return url


def main():
    graphql_client = GraphQLClient()
    speech_client = SpeechRestClient()
    s3_client = boto3.client('s3')
    html_template = Environment(loader=FileSystemLoader('./data', encoding='utf8')) \
        .get_template('transcription_template.html')

    operation_ids = speech_client.list_()
    LOGGER.info(f'found total {len(operation_ids)} operation ids: {operation_ids}')

    for operation_id in operation_ids:
        try:
            data = speech_client.get(operation_id)
        except Exception as e:
            LOGGER.warning(f'failed to fetch operation result for {operation_id}: {e}')
            continue

        job_name = data['metadata']['job_name']
        minutes_id = f'Minutes:{job_name}'

        try:
            minutes = graphql_client.get(minutes_id)
        except Exception as e:
            LOGGER.warning(f'failed to get minutes: {e}')
            continue

        json_fp = f'./voice/{job_name}.json'
        html_fp = f'./voice/{job_name}.html'
        s3_json_fp = f'minutes/{job_name}.json'
        s3_html_fp = f'minutes/{job_name}.html'
        s3_html_url = f'https://text.politylink.jp/{s3_html_fp}'

        with open(json_fp, 'w') as f:
            json.dump(data, f, ensure_ascii=False)
        LOGGER.info(f'saved JSON result in {json_fp}')

        html = build_html(html_template, data, minutes)
        with open(html_fp, 'w', encoding="utf-8") as f:
            f.write(html)
        LOGGER.info(f'saved HTML in {html_fp}')

        if args.publish:
            s3_client.upload_file(json_fp, 'politylink-text', s3_json_fp, ExtraArgs={"ContentType": "application/json"})
            s3_client.upload_file(html_fp, 'politylink-text', s3_html_fp, ExtraArgs={"ContentType": "text/html"})
            url = build_url(s3_html_url)
            graphql_client.merge(url)
            graphql_client.link(url.id, minutes.id)
            LOGGER.info(f'published HTML to S3: {s3_html_url}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='GCPの文字起こしAPIの結果をRESTで取得する')
    parser.add_argument('-p', '--publish', help='S3にtextファイルを送る', action='store_true')
    parser.add_argument('-v', '--verbose', action='store_true')
    args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO)
    main()
