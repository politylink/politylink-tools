import argparse
import logging

import cv2
import numpy as np
import pandas as pd
from tqdm import tqdm

LOGGER = logging.getLogger(__name__)
PIXEL_DIFF_THRESH = 10


def get_frame(cap, sec):
    fps = cap.get(cv2.CAP_PROP_FPS)
    cap.set(cv2.CAP_PROP_POS_FRAMES, round(sec * fps))
    ret, frame = cap.read()
    return frame


def calc_frame_diff_rate(frame1, frame2):
    # can not use np.abs because of uint8 overflow
    frame_diff = np.minimum(np.subtract(frame1, frame2), np.subtract(frame2, frame1))
    frame_diff_bin = np.where(frame_diff <= PIXEL_DIFF_THRESH, 0, 1)
    diff_rate = frame_diff_bin.sum() / frame_diff_bin.size
    return diff_rate


def main(video_fp, diff_fp):
    LOGGER.info(f'load {video_fp}')
    cap = cv2.VideoCapture(video_fp)
    fps = cap.get(cv2.CAP_PROP_FPS)
    frame_count = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    duration = int(frame_count / fps)
    LOGGER.info(f'fps={fps}, frames={frame_count}, duration={duration}')

    records = []
    prev_frame = get_frame(cap, 0)
    for sec in tqdm(range(duration)):
        frame = get_frame(cap, sec)
        records.append({
            'sec': sec,
            'diff': calc_frame_diff_rate(prev_frame, frame)
        })
        prev_frame = frame
    df = pd.DataFrame(records, columns=['sec', 'diff'])
    df.to_csv(diff_fp, index=False)
    LOGGER.info(f'saved {diff_fp}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='１秒ごとに前のフレームとの差分率を算出してCSVに保存する')
    parser.add_argument('--video', help='動画ファイル（mp4）', required=True)
    parser.add_argument('--diff', help='差分ファイル（csv）', required=True)
    parser.add_argument('-v', '--verbose', action='store_true')
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO)
    main(args.video, args.diff)
