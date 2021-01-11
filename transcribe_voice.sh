set -ex

FILE_NAME="video_1"

VOICE_URL="https://webtv-vod.live.ipcasting.jp/vod/mp4:5992.mp4/playlist.m3u8"
OUTPUT_FILE_MP4="${FILE_NAME}.mp4"
ffmpeg -protocol_whitelist file,http,https,tcp,tls,crypto -i "${VOICE_URL}" -c copy "${OUTPUT_FILE_MP4}"

OUTPUT_FILE_WAV="${FILE_NAME}.wav"
ffmpeg -i "${OUTPUT_FILE_MP4}" "${OUTPUT_FILE_WAV}"

GCS_VOICE_PATH="gs://politylink-speech/voice/${FILE_NAME}.wav"
gsutil cp "${OUTPUT_FILE_WAV}" "${GCS_VOICE_PATH}"

BUCKET="politylink-speech"
SAVE_PATH='../data/${FILE_NAME}.json'
python transcribe_voice.py "${BUCKET}" "voice/${OUTPUT_FILE_WAV}" "${SAVE_PATH}"

GCS_TRANS_PATH="gs://politylink-speech/transcription/${FILE_NAME}.json"
gsutil cp "${SAVE_PATH}" "${GCS_TRANS_PATH}"
