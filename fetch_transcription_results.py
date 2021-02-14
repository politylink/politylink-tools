import argparse
import json
import logging
import os

import requests

LOGGER = logging.getLogger(__name__)


class SpeechRestClient:
    """
    REST client to access GCP Speech-to-Text results
    https://cloud.google.com/speech-to-text/docs/reference/rest
    """

    def __init__(self):
        self.api_key = os.environ['GCP_API_KEY']

    def list(self):
        """
        return currently available operation names
        """

        url = f'https://speech.googleapis.com/v1/operations?key={self.api_key}'
        response = requests.get(url)
        data = response.json()
        return list(map(lambda x: x['name'], data['operations']))

    def get(self, op_name):
        """
        return finished operation result
        """

        url = f'https://speech.googleapis.com/v1/operations/{op_name}?key={self.api_key}'
        response = requests.get(url)
        data = response.json()

        if ('done' not in data) or (not data['done']):
            raise ValueError('operation is not finished')

        if 'error' in data:
            raise ValueError(f'operation failed: {data["error"]}')

        data['metadata']['job_id'] = data['metadata']['uri'].split('/')[-1].split('.')[0]
        return data


def main():
    speech_client = SpeechRestClient()

    op_names = speech_client.list()
    LOGGER.info(f'found total {len(op_names)} operations: {op_names}')

    for op_name in op_names:
        try:
            data = speech_client.get(op_name)
        except Exception as e:
            LOGGER.warning(f'failed to fetch operation result for {op_name}: {e}')
            continue
        LOGGER.info(f'fetched transcription result for {op_name}')
        json_fp = f'./voice/{data["metadata"]["job_id"]}.json'
        with open(json_fp, 'w') as f:
            json.dump(data, f, ensure_ascii=False)
        LOGGER.info(f'saved JSON result in {json_fp}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='GCPの文字起こしAPIの結果をRESTで取得する')
    parser.add_argument('-v', '--verbose', action='store_true')
    args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO)
    main()
