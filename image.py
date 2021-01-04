import argparse
import logging

import boto3
import requests
import time
from tqdm import tqdm

from politylink.graphql.client import GraphQLClient

LOGGER = logging.getLogger(__name__)

"""
requires ~/.aws/credentials
https://boto3.amazonaws.com/v1/documentation/api/latest/guide/quickstart.html
"""


def main():
    client = GraphQLClient(url="https://graphql.politylink.jp/")
    members = client.get_all_members(fields=['id', 'image'])
    s3 = boto3.resource('s3')
    for member in tqdm(members):
        response = requests.get(member['image'])
        object_key = 'member/{}.jpg'.format(member.id.split(':')[-1])
        s3.Bucket('politylink').put_object(Key=object_key, Body=response.content)
        time.sleep(1)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='ImageをS3にアップロードする')
    parser.add_argument('-v', '--verbose', action='store_true')
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO)
    logging.getLogger('sgqlc').setLevel(logging.INFO)
    main()
