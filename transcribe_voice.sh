set -ex

# Need to set the environment variable "GOOGLE_APPLICATION_CREDENTIALS".

FILE_NAME="video_sangiin_20210118"

VOICE_URL="https://webtv-vod.live.ipcasting.jp/vod/mp4:6097.mp4/playlist.m3u8"
OUTPUT_FILE_MP4="./data/${FILE_NAME}.mp4"
ffmpeg -protocol_whitelist file,http,https,tcp,tls,crypto -i "${VOICE_URL}" -c copy "${OUTPUT_FILE_MP4}"

OUTPUT_FILE_WAV="./data/${FILE_NAME}.wav"
ffmpeg -i "${OUTPUT_FILE_MP4}" "${OUTPUT_FILE_WAV}"

BUCKET="politylink-speech"
GCS_VOICE_PATH="gs://${BUCKET}/voice/${OUTPUT_FILE_WAV}"
gsutil cp "${OUTPUT_FILE_WAV}" "${GCS_VOICE_PATH}"

SAVE_PATH="./data/${FILE_NAME}.json"
python transcribe_voice.py "${OUTPUT_FILE_WAV}" "${GCS_VOICE_PATH}" "${SAVE_PATH}"

GCS_TRANS_PATH="gs://${BUCKET}/transcription/${FILE_NAME}.json"
gsutil cp "${SAVE_PATH}" "${GCS_TRANS_PATH}"
