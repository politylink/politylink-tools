import argparse
import logging

import boto3
import os
import requests
import time
from collections import defaultdict
from tqdm import tqdm
from wand.image import Image as wandImage

from politylink.graphql.client import GraphQLClient

LOGGER = logging.getLogger(__name__)

"""
Refer: https://qiita.com/tomtsutom0122/items/bb5acaee4f4f7820124c

requires ~/.aws/credentials
https://boto3.amazonaws.com/v1/documentation/api/latest/guide/quickstart.html
"""

def build_thumbnail(data, local_path):
    image = wandImage(blob=data, resolution=300, format="pdf")
    if len(image.sequence) > 1:
        # Select the first page of the pdf
        image = wandImage(image.sequence[0])
    image.transform(resize='640')
    image.crop(width=640, height=360)
    image.alpha_channel = 'remove'
    image.save(filename=local_path)
    return image.make_blob()

def main():
    client = GraphQLClient(url="https://graphql.politylink.jp/")
    bills = client.get_all_bills(fields=['id', 'urls'])
    stats = defaultdict(int)
    s3 = boto3.resource('s3')
    os.makedirs("./image/bill", exist_ok=True)

    for bill in tqdm(bills):
        summary_pdf = next(filter(lambda x: x.title == "概要PDF", bill.urls), None)
        LOGGER.debug(f'Processing ... {bill.id}')
        if summary_pdf:
            stats['process'] += 1
            id_ = bill.id.split(':')[-1]
            local_path = f'./image/bill/{id_}.png'
            try:
                response = requests.get(summary_pdf.url)
                thumbnail = build_thumbnail(response, local_path)
            except Exception as e:
                LOGGER.warning(f'failed to convert summary pdf to png: {e}')
                stats['fail'] += 1
                continue
            if args.publish:
                object_key = f'bill/{id_}.png'
                s3.Bucket('politylink').put_object(Key=object_key, Body=thumbnail, ContentType="image/png")
            time.sleep(1)
    LOGGER.info('processed {} bills ({} success, {} fail)'.format(
        stats['process'], stats['process'] - stats['fail'], stats['fail']
    ))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='法律案概要pdfのサムネイルをS3にアップロードする')
    parser.add_argument('-p', '--publish', help='画像をS3にアップロードする', action='store_true')
    parser.add_argument('-v', '--verbose', action='store_true')
    args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO)
    logging.getLogger('sgqlc').setLevel(logging.INFO)
    logging.getLogger('botocore').setLevel(logging.WARNING)
    main()
