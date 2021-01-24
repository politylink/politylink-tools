import json
import os
import re

import sys
from google.cloud import speech_v1p1beta1
from pydub import AudioSegment

from politylink.graphql.client import GraphQLClient


def insert_punctuation(text):
    inserted_text = re.sub(r'ました(。)?', 'ました。', text)
    inserted_text = re.sub(r'します(。)?', 'します。', inserted_text)
    inserted_text = re.sub(r'きます(。)?', 'きます。', inserted_text)
    inserted_text = re.sub(r'います(。)?', 'います。', inserted_text)
    inserted_text = re.sub(r'であります(。)?', 'であります。', inserted_text)
    inserted_text = re.sub(r'となります(。)?', 'となります。', inserted_text)
    inserted_text = re.sub(r'参ります(。)?', '参ります。', inserted_text)

    return inserted_text


def format_transcription(transcription_json):
    formatted_texts = ''
    for i in range(len(transcription_json)):
        text = transcription_json[f'speech{i}']['transcript']
        inserted_text = insert_punctuation(text)
        formatted_texts += inserted_text

    return formatted_texts


def get_voice_info(local_file_path):
    if os.path.isfile(local_file_path):
        sound = AudioSegment.from_file(local_file_path, format='mp3')
        channels = sound.channels
        rate = sound.frame_rate
        duration = sound.duration_seconds
    else:
        print(f'Error: "{local_file_path}" does not exist.')
        sys.exit()

    print('\n-*-------------- audio info --------------*-')
    print(f'filename   : {local_file_path}')
    print(f'sampleRate : {str(rate)}')
    print(f'playtime   : {str(duration)} [sec]')
    print(f'channels   : {str(channels)}')
    print('-*--------------------------------------------*-')

    return rate, duration, channels


def get_speech_contexts(json_path):
    speech_contexts = list(json.load(open(json_path, 'r')).values())
    gql_client = GraphQLClient()
    members = gql_client.get_all_members(['name'])
    member_context = {
        'phrases': [member.name for member in members],
        'boost': 20.0
    }
    speech_contexts.append(member_context)
    return speech_contexts


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
    print(f'Raw data is Saved in {save_path}')

    # Formatted transcription data
    # save_path extension converts '.json' to '.txt'
    formatted_texts = format_transcription(transcription_json)
    save_text_path = os.path.splitext(save_path)[0] + '.txt'
    with open(save_text_path, 'w') as f:
        f.write(formatted_texts)
    print(f'Formatted data is Saved in {save_text_path}')


def transcribe_voice(local_file_path, gcs_file_path, save_path):
    # fetch voice content
    rate, duration, channels = get_voice_info(local_file_path)

    # cloud cost
    print(f'The cost of the cloud is around ${0.008 * duration / 15:.2f}')

    # set boosting words
    speech_contexts = get_speech_contexts('./data/speech_contexts.json')

    # set config of GCP speech-to-text
    config = {
        'encoding': 'MP3',
        'sample_rate_hertz': rate,
        'language_code': 'ja-JP',
        'audio_channel_count': channels,
        'enable_automatic_punctuation': True,
        'speech_contexts': speech_contexts,
        'diarization_config': {
            "enable_speaker_diarization": True,
            "min_speaker_count": 1,
            "max_speaker_count": 10}
    }

    # set GCS URI of voice data
    audio = {'uri': gcs_file_path}

    # transcribe voice data
    client = speech_v1p1beta1.SpeechClient()
    operation = client.long_running_recognize(config=config, audio=audio)
    print(f'operation name = {operation.operation.name}')
    response = operation.result(timeout=duration)
    print('\n-*- transcribe result -*-')

    # save transcribed text data
    save_transcription(response, save_path)


if __name__ == '__main__':
    args = sys.argv
    if len(args) == 4:
        transcribe_voice(args[1], args[2], args[3])
    else:
        sys.exit('Error: invalid argument')
