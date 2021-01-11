import json
import magic
import os
import sys

from google.cloud import speech
from google.cloud import storage as gcs
from mutagen.flac import FLAC
from pydub import AudioSegment

GOOGLE_APPLICATION_CREDENTIALS = 'Set your GCP service account key file (json format)'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = GOOGLE_APPLICATION_CREDENTIALS

PROJECT = 'politylink'


def fetch_from_gcs(bucket_name, file_name):
    client = gcs.Client(project=PROJECT)
    bucket = client.get_bucket(bucket_name)
    blob = gcs.Blob(file_name, bucket)
    content = blob.download_as_bytes()
    print(f'Fetched data from {bucket_name}/{file_name}')

    return content


def get_voice_info(content, file_name):
    mime = magic.Magic(mime=True).from_buffer(content)
    if mime == 'audio/x-wav' and '.wav' in file_name:
        encoding = 'LINEAR16'
        sound = AudioSegment(content)
        rate = sound.frame_rate
        length = sound.duration_seconds
    elif mime == 'audio/x-flac' and '.flac' in file_name:
        encoding = 'FLAC'
        with open(file_name, 'wb') as f:
            f.write(content)
            f.close()
        sound = FLAC(file_name).info
        rate = sound.sample_rate
        length = sound.length
    else:
        print('Acceptable type is only "wav" or "flac".')
        sys.exit()

    print('\n-*- audio info -*-')
    print('filename   : ' + file_name)
    print('mimetype   : ' + mime)
    print('sampleRate : ' + str(rate))
    print('playtime   : ' + str(length) + '[sec]')
    print('channels : ' + str(sound.channels))
    print('\nWaiting for operation to complete...')

    return encoding, mime, rate, length, sound


def save_transcription(response, save_path):
    transcription_json = {}
    for index, item in enumerate(response.results):
        transcription_json[f'speech{index}'] = {
            'confidence': item.alternatives[0].confidence,
            'transcript': item.alternatives[0].transcript
        }

    with open(save_path, 'w') as f:
        json.dump(
            transcription_json, f, indent=4, ensure_ascii=False)
    print(f'Saved in {save_path}')


def transcribe_voice(bucket_name, file_name, save_path):
    client = speech.SpeechClient()
    audio = {'uri': f'gs://{bucket_name}/{file_name}'}

    # fetch voice content
    content = fetch_from_gcs(bucket_name, file_name)
    encoding, mime, rate, length, sound = get_voice_info(content, file_name)

    # cloud cost
    print(f'The cost of the cloud is around ${0.008*length/15:.2f}')

    # set config of GCP speech-to-text
    config = {
        'encoding': encoding,
        'sample_rate_hertz': rate,
        'language_code': 'ja-JP',
        'audio_channel_count': sound.channels,
        'enable_automatic_punctuation': True,
        'diarization_config': {
            "enable_speaker_diarization": True, "min_speaker_count": 6,
            "max_speaker_count": 2}
    }

    # switch methods according to playback
    if length < 60:
        response = client.recognize(config=config, audio=audio)
    else:
        operation = client.long_running_recognize(config=config, audio=audio)
        response = operation.result(timeout=length)
    print('\n-*- transcribe result -*-')

    save_transcription(response, save_path)


if __name__ == '__main__':
    args = sys.argv
    if len(args) == 4:
        transcribe_voice(args[1], args[2], args[3])
    else:
        sys.exit('Error: invalid argument')
