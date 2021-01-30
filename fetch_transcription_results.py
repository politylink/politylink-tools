import argparse
import json
import logging
import os

import requests

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

        if not data['done']:
            raise ValueError('operation is not finished')

        if 'error' in data:
            raise ValueError(f'operation failed: {data["error"]}')

        data['metadata']['job_name'] = data['metadata']['uri'].split('/')[-1].replace('.mp3', '')
        return data


def parse_time_str(time_str):
    return float(time_str[:-1])  # remove last "s"


def format_transcripts(data):
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

        if start_time - last_end_time >= 3:  # likely speaker has changed
            if buffer:  # flush buffer
                transcripts.append(buffer)
                buffer = ''

        buffer += formatted_text
        last_end_time = end_time

    if buffer:
        transcripts.append(buffer)
    return transcripts


def main():
    client = SpeechRestClient()
    ids = client.list_()
    LOGGER.info(f'found total {len(ids)} operation ids: {ids}')
    for id_ in ids:
        try:
            data = client.get(id_)
        except Exception as e:
            LOGGER.warning(f'failed to fetch operation result for {id_}: {e}')
            continue

        json_fp = './voice/{}.json'.format(data['metadata']['job_name'])
        text_fp = json_fp.replace('.json', '.txt')

        with open(json_fp, 'w') as f:
            json.dump(data, f, ensure_ascii=False)
        LOGGER.info(f'saved raw result in {json_fp}')

        transcripts = format_transcripts(data)
        with open(text_fp, 'w') as f:
            for text in transcripts:
                f.write(f'{text}\n\n')
        LOGGER.info(f'saved text result in {text_fp}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='GCPの文字起こしAPIの結果をRESTで取得する')
    parser.add_argument('-v', '--verbose', action='store_true')
    args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO)
    main()
