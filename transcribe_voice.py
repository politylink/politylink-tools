import argparse
import json
import logging
import os
import re

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


def format_transcription(transcription_json):
    formatted_texts = ''
    for i in range(len(transcription_json)):
        text = transcription_json[f'speech{i}']['transcript']
        inserted_text = insert_punctuation(text)
        formatted_texts += inserted_text

    return formatted_texts


def insert_punctuation(text):
    inserted_text = re.sub(r'ました(。)?', 'ました。', text)
    inserted_text = re.sub(r'します(。)?', 'します。', inserted_text)
    inserted_text = re.sub(r'きます(。)?', 'きます。', inserted_text)
    inserted_text = re.sub(r'います(。)?', 'います。', inserted_text)
    inserted_text = re.sub(r'であります(。)?', 'であります。', inserted_text)
    inserted_text = re.sub(r'となります(。)?', 'となります。', inserted_text)
    inserted_text = re.sub(r'参ります(。)?', '参ります。', inserted_text)

    return inserted_text


def save_transcription(response, save_path):
    transcription_json = {}
    for index, item in enumerate(response.results):
        transcription_json[f'speech{index}'] = {
            'confidence': item.alternatives[0].confidence,
            'transcript': item.alternatives[0].transcript
        }

    # Raw transcription data
    with open(save_path, 'w') as f:
        json.dump(
            transcription_json, f, indent=4, ensure_ascii=False)
    LOGGER.info(f'saved raw json result in {save_path}')

    # Formatted transcription data
    # save_path extension converts '.json' to '.txt'
    formatted_texts = format_transcription(transcription_json)
    save_text_path = os.path.splitext(save_path)[0] + '.txt'
    with open(save_text_path, 'w') as f:
        f.write(formatted_texts)
    print(f'saved formatted text in {save_text_path}')


def main(local_file_path, gcs_file_path, contexts_file_path=None, result_file_path=None):
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
    duration = float(media_info['duration'])
    LOGGER.info(f'GCP cost will be around ${0.008 * duration / 15:.2f}')

    if result_file_path:
        LOGGER.debug(f'wait maximum {duration} secs until the result is ready')
        response = operation.result(timeout=duration)
        save_transcription(response, result_file_path)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='音声ファイルをGCPのSpeechToText APIに投げる')
    parser.add_argument('-l', '--local', help='ローカルの音声ファイル（.mp3）', required=True)
    parser.add_argument('-g', '--gcs', help='Google Cloud Storageの音声ファイル（.mp3）', required=True)
    parser.add_argument('-c', '--contexts', help='文字起こし用のカスタム辞書（SpeechContexts）', default='./data/speech_contexts.json')
    parser.add_argument('-r', '--result', help='結果を保存するローカルファイル(.json)。与えられた場合、文字起こしの結果が出るまで同期して待つ', default=None)
    parser.add_argument('-v', '--verbose', action='store_true')
    args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO)
    logging.getLogger('sgqlc').setLevel(logging.INFO)
    main(args.local, args.gcs, args.result)
