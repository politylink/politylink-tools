set -ex

# Need to set the environment variable "GOOGLE_APPLICATION_CREDENTIALS".

FILE_NAME="sangiin_20210121"
VIDEO_URL="https://webtv-vod.live.ipcasting.jp/vod/mp4:6109.mp4/playlist.m3u8"

LOCAL_VIDEO_PATH="./voice/${FILE_NAME}.mp4"
LOCAL_VOICE_PATH="./voice/${FILE_NAME}.wav"
LOCAL_TRANS_PATH="./voice/${FILE_NAME}.json"

GCS_BUCKET="politylink-speech-mu"
GCS_VOICE_PATH="gs://${GCS_BUCKET}/voice/${FILE_NAME}.mp3"
GCS_TRANS_PATH="gs://${GCS_BUCKET}/transcription/${FILE_NAME}.json"

ffmpeg -protocol_whitelist file,http,https,tcp,tls,crypto -i "${VIDEO_URL}" -c copy "${LOCAL_VIDEO_PATH}"
ffmpeg -i "${LOCAL_VIDEO_PATH}" "${LOCAL_VOICE_PATH}"
gsutil cp "${LOCAL_VOICE_PATH}" "${GCS_VOICE_PATH}"
python transcribe_voice.py "${LOCAL_VOICE_PATH}" "${GCS_VOICE_PATH}" "${LOCAL_TRANS_PATH}"
gsutil cp "${LOCAL_TRANS_PATH}" "${GCS_TRANS_PATH}"
