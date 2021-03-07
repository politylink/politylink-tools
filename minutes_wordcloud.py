import argparse
import json
import logging
from datetime import date, timedelta

import boto3
import requests
from tqdm import tqdm
from wordcloud import WordCloud

from politylink.graphql.client import GraphQLClient
from utils import date_type

LOGGER = logging.getLogger(__name__)
GRAPHQL_URL = 'https://graphql.politylink.jp'
WORDCLOUD_URL = 'https://api.politylink.jp/tf_idf'
WORDCLOUD_PARAMS = {
    'font_path': '/system/Library/Fonts/ヒラギノ明朝 ProN.ttc',
    'background_color': 'white',
    'height': 400,
    'width': 600
}

gql_client = GraphQLClient(url=GRAPHQL_URL)
s3_client = boto3.client('s3')


def to_date(dt):
    return date(year=dt.year, month=dt.month, day=dt.day)


def to_date_str(dt):
    return dt.strftime('%Y-%m-%d')


def fetch_tfidf(minutes):
    """
    call wordcloud server to fetch tfidf
    raises exception when request failed of response is empty
    """

    start_date = to_date(minutes.start_date_time)
    end_date = start_date + timedelta(days=1)
    request_param = {
        'committee': minutes.name,
        'start': to_date_str(start_date),
        'end': to_date_str(end_date),
        'items': 30,
    }
    LOGGER.debug(f'call wordcloud sever: {request_param}')
    response = requests.post(
        WORDCLOUD_URL,
        json.dumps(request_param, ensure_ascii=False).encode('utf-8'),
        headers={'Content-Type': 'application/json'}
    )
    tfidf = response.json()[0]['tfidf']
    if not tfidf:
        raise ValueError(f'empty tfidf returned')
    return tfidf


def process(minutes):
    LOGGER.debug(f'process {minutes.id}')

    try:
        tfidf = fetch_tfidf(minutes)
    except Exception:
        LOGGER.exception(f'failed to fetch tfidf from wordcloud server')
        return

    id_ = minutes.id.split(':')[-1]
    local_path = f'./wordcloud/minutes/{id_}.jpg'
    s3_path = f'minutes/{id_}.jpg'

    wordcloud = WordCloud(**WORDCLOUD_PARAMS).generate_from_frequencies(tfidf)
    wordcloud.to_file(local_path)
    LOGGER.info(f'saved wordcloud to {local_path}')

    if args.publish:
        s3_client.upload_file(local_path, 'politylink', s3_path, ExtraArgs={'ContentType': 'image/jpeg'})
        LOGGER.info(f'published wordcloud to {s3_path}')


def is_target_minutes(minutes):
    if not minutes.ndl_min_id:
        return False
    dt = to_date(minutes.start_date_time)
    if not (args.start_date <= dt <= args.end_date):
        return False
    return True


def main():
    minutes_list = gql_client.get_all_minutes(fields=['id', 'name', 'start_date_time', 'ndl_min_id'])
    LOGGER.info(f'loaded {len(minutes_list)} minutes from GraphQL')
    minutes_list = list(filter(lambda x: is_target_minutes(x), minutes_list))
    LOGGER.info(f'filtered {len(minutes_list)} target minutes')

    for minutes in tqdm(minutes_list):
        process(minutes)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Minutesのワードクラウドを生成する')
    parser.add_argument('-s', '--start_date', help='開始日（例: 2020-01-01）', type=date_type, required=True)
    parser.add_argument('-e', '--end_date', help='終了日（例: 2020-01-01）', type=date_type, required=True)
    parser.add_argument('-p', '--publish', help='画像をS3にアップロードする', action='store_true')
    parser.add_argument('-v', '--verbose', action='store_true')
    args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO)
    logging.getLogger('sgqlc').setLevel(logging.WARNING)
    logging.getLogger('botocore').setLevel(logging.WARNING)
    logging.getLogger('s3transfer').setLevel(logging.WARNING)
    main()
