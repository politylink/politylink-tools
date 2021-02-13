set -ex

# Requirements:
#   ffmpeg: https://www.johnvansickle.com/ffmpeg/faq/
#   gsutil: https://cloud.google.com/storage/docs/gsutil_install
#   export GOOGLE_APPLICATION_CREDENTIALS=${local_gcp_credential_file_path}

JOB_ID=$1
VIDEO_URL=$2

LOCAL_VIDEO_PATH="./voice/${JOB_ID}.mp4"
LOCAL_VOICE_PATH="./voice/${JOB_ID}.mp3"
LOCAL_DIFF_PATH="./voice/${JOB_ID}.diff"

GCS_BUCKET="politylink-speech-mu"
GCS_VOICE_PATH="gs://${GCS_BUCKET}/voice/${JOB_ID}.mp3"

yes | ffmpeg -protocol_whitelist file,http,https,tcp,tls,crypto -i "${VIDEO_URL}" -c copy "${LOCAL_VIDEO_PATH}"
yes | ffmpeg -i "${LOCAL_VIDEO_PATH}" -ar 44100 -ac 1 "${LOCAL_VOICE_PATH}"
yes | gsutil cp "${LOCAL_VOICE_PATH}" "${GCS_VOICE_PATH}"
yes | poetry run python transcribe_voice.py --local "${LOCAL_VOICE_PATH}" --gcs "${GCS_VOICE_PATH}"
yes | poetry run python diff_video.py --video "${LOCAL_VIDEO_PATH}" --diff "${LOCAL_DIFF_PATH}"
yes | rm -f "${LOCAL_VIDEO_PATH}" "${LOCAL_VOICE_PATH}"
