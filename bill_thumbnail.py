import argparse
import logging
from collections import defaultdict
from pathlib import Path

import boto3
import requests
import time
from tqdm import tqdm
from wand.image import Image as wandImage

from politylink.graphql.client import GraphQLClient

LOGGER = logging.getLogger(__name__)

"""
requires ~/.aws/credentials
https://boto3.amazonaws.com/v1/documentation/api/latest/guide/quickstart.html
"""


def save_thumbnail(pdf_path, thumbnail_path):
    """
    https://qiita.com/tomtsutom0122/items/bb5acaee4f4f7820124c
    """

    image = wandImage(filename=f'{pdf_path}[0]', resolution=300, format="pdf")
    image.transform(resize='640')
    image.crop(width=640, height=320)
    image.alpha_channel = 'remove'
    image.save(filename=thumbnail_path)
    return image.make_blob()


def get_maybe_summary_pdf(bill):
    for url in bill.urls:
        if url.title == '概要PDF':
            url_str = url.url
            if 'shugiin.go.jp' in url_str:  # adhoc-fix for requests module failure with HTTP
                url_str.replace('http://', 'https://')
            return url_str
    return None


def main():
    gql_client = GraphQLClient(url="https://graphql.politylink.jp/")
    s3_client = boto3.client('s3')

    bills = gql_client.get_all_bills(fields=['id', 'urls'])
    LOGGER.info(f'fetched {len(bills)} bills')

    stats = defaultdict(int)
    for bill in tqdm(bills):
        LOGGER.debug(f'check {bill.id}')

        id_body = bill.id.split(':')[-1]
        local_path = Path(f'./image/bill/{id_body}.png')
        s3_path = Path(f'bill/{id_body}.png')
        pdf_path = local_path.with_suffix('.pdf')
        local_path.parent.mkdir(parents=True, exist_ok=True)

        if local_path.exists() and not args.overwrite:
            LOGGER.debug(f'{local_path} already exists')
            continue

        maybe_summary_pdf = get_maybe_summary_pdf(bill)
        if not maybe_summary_pdf:
            LOGGER.debug('summary PDF does not exist')
            continue
        summary_pdf = maybe_summary_pdf

        stats['process'] += 1
        time.sleep(1)

        response = requests.get(summary_pdf)
        if not response.ok:
            LOGGER.warning(f'failed to fetch {summary_pdf}')
            stats['fail'] += 1
            continue
        with open(pdf_path, 'wb') as f:
            f.write(response.content)
        LOGGER.debug(f'saved {pdf_path}')

        try:
            save_thumbnail(pdf_path, local_path)
        except Exception:
            LOGGER.exception(f'failed to save {summary_pdf}')
            stats['fail'] += 1
            continue
        LOGGER.debug(f'saved {local_path}')

        if args.publish:
            s3_client.upload_file(str(local_path), 'politylink', str(s3_path), ExtraArgs={'ContentType': 'image/png'})
            LOGGER.debug(f'published {s3_path}')

    LOGGER.info('processed {} bills ({} success, {} fail)'.format(
        stats['process'], stats['process'] - stats['fail'], stats['fail']
    ))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='法律案のサムネイルを概要PDFから生成する')
    parser.add_argument('-p', '--publish', help='画像をS3にアップロードする', action='store_true')
    parser.add_argument('-o', '--overwrite', help='画像を再生成する', action='store_true')
    parser.add_argument('-v', '--verbose', action='store_true')
    args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO)
    logging.getLogger('sgqlc').setLevel(logging.INFO)
    logging.getLogger('botocore').setLevel(logging.WARNING)
    main()
