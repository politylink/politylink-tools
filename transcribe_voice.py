import argparse
import json
import logging

from google.cloud import speech_v1p1beta1
from pydub.utils import mediainfo

from politylink.graphql.client import GraphQLClient

LOGGER = logging.getLogger(__name__)


def get_speech_contexts(json_path=None):
    speech_contexts = []

    # add contexts from json
    if json_path:
        speech_contexts += json.load(open(json_path, 'r')).values()

    # add contexts from GraphQL
    gql_client = GraphQLClient()
    members = gql_client.get_all_members(['name'])
    if members:
        member_context = {
            'phrases': [member.name for member in members],
            'boost': 20.0
        }
        speech_contexts.append(member_context)

    return speech_contexts


def main(local_file_path, gcs_file_path, contexts_file_path=None):
    media_info = mediainfo(local_file_path)
    speech_client = speech_v1p1beta1.SpeechClient()
    config = {
        'encoding': 'MP3',
        'sample_rate_hertz': int(media_info['sample_rate']),
        'language_code': 'ja-JP',
        'audio_channel_count': int(media_info['channels']),
        'enable_automatic_punctuation': True,
        'speech_contexts': get_speech_contexts(contexts_file_path),
        'diarization_config': {
            "enable_speaker_diarization": True,
            "min_speaker_count": 1,
            "max_speaker_count": 10}
    }
    audio = {
        'uri': gcs_file_path
    }
    operation = speech_client.long_running_recognize(config=config, audio=audio)
    LOGGER.info(f'submitted Speech-to-Text operation: id={operation.operation.name}')
    gcp_cost = 0.008 * float(media_info['duration']) / 15
    LOGGER.info(f'GCP cost will be around ${gcp_cost:.2f}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='音声ファイルをGCPのSpeechToText APIに投げる')
    parser.add_argument('-l', '--local', help='ローカルの音声ファイル（.mp3）', required=True)
    parser.add_argument('-g', '--gcs', help='Google Cloud Storageの音声ファイル（.mp3）', required=True)
    parser.add_argument('-c', '--contexts', help='文字起こし用のカスタム辞書（SpeechContexts）', default='./data/speech_contexts.json')
    parser.add_argument('-v', '--verbose', action='store_true')
    args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO)
    logging.getLogger('sgqlc').setLevel(logging.INFO)
    main(args.local, args.gcs)
