import argparse
import logging

import boto3
import requests
import time
from collections import defaultdict
from tqdm import tqdm
from wand.image import Image as wandImage
# https://qiita.com/tomtsutom0122/items/bb5acaee4f4f7820124c

from politylink.graphql.client import GraphQLClient

LOGGER = logging.getLogger(__name__)

"""
requires ~/.aws/credentials
https://boto3.amazonaws.com/v1/documentation/api/latest/guide/quickstart.html
"""

def buildThumbnail(response, uuid):
    data = response
    image = wandImage(blob=data, resolution=300, format="pdf")
    if len(image.sequence) > 1:
        # Select the first page of the pdf
        image = wandImage(image.sequence[0])
    image.transform(resize='640')
    image.crop(width=640, height=360)
    image.alpha_channel = 'remove'
    image.format = 'png'
    # image.save(filename=uuid + '.png')
    return image.make_blob()

def main():
    client = GraphQLClient(url="https://graphql.politylink.jp/")
    bills = client.get_all_bills(fields=['id', 'urls'])
    #s3 = boto3.resource('s3')
    stats = defaultdict(int)
    for bill in tqdm(bills):
        summary_pdf = next(filter(lambda x: x['title'] == "概要PDF", bill['urls']), None)
        LOGGER.debug(f'Processing ... {bill}')
        if summary_pdf:
            stats['process'] += 1
            try:
                response = requests.get(summary_pdf['url'])
                thumbnail = buildThumbnail(response, bill.id)
            except Exception as e:
                LOGGER.warning(f'failed to convert summary pdf to png: {e}')
                stats['fail'] += 1
                continue
            object_key = 'bill/{}.png'.format(bill.id.split(':')[-1])
            #s3.Bucket('politylink').put_object(Key=object_key, Body=thumbnail, ContentType="image/png")
            #time.sleep(1)
    LOGGER.info('processed {} bills ({} success, {} fail)'.format(
        stats['process'], stats['process'] - stats['fail'], stats['fail']
    ))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='法律案概要pdfのサムネイルをS3にアップロードする')
    parser.add_argument('-v', '--verbose', action='store_true')
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO)
    logging.getLogger('sgqlc').setLevel(logging.INFO)
    main()
