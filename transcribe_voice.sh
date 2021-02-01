set -ex

# Need to set the environment variable "GOOGLE_APPLICATION_CREDENTIALS".

JOB_NAME=$1
VIDEO_URL=$2

LOCAL_VIDEO_PATH="./voice/${JOB_NAME}.mp4"
LOCAL_VOICE_PATH="./voice/${JOB_NAME}.mp3"

GCS_BUCKET="politylink-speech-mu"
GCS_VOICE_PATH="gs://${GCS_BUCKET}/voice/${JOB_NAME}.mp3"

yes | ffmpeg -protocol_whitelist file,http,https,tcp,tls,crypto -i "${VIDEO_URL}" -c copy "${LOCAL_VIDEO_PATH}"
yes | ffmpeg -i "${LOCAL_VIDEO_PATH}" -ar 44100 -ac 1 "${LOCAL_VOICE_PATH}"
yes | gsutil cp "${LOCAL_VOICE_PATH}" "${GCS_VOICE_PATH}"
yes | poetry run python transcribe_voice.py --local "${LOCAL_VOICE_PATH}" --gcs "${GCS_VOICE_PATH}"
yes | rm -f "${LOCAL_VIDEO_PATH}" "${LOCAL_VOICE_PATH}"
