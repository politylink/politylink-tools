import argparse
import json
import logging
from datetime import date, timedelta

import boto3
import requests
from tqdm import tqdm
from wordcloud import WordCloud

from politylink.elasticsearch.client import ElasticsearchClient
from politylink.graphql.client import GraphQLClient
from politylink.graphql.schema import Minutes
from politylink.nlp.utils import filter_by_pos, WORDCLOUD_POS_TAGS
from utils import date_type

LOGGER = logging.getLogger(__name__)
WORDCLOUD_URL = 'https://api.politylink.jp/tf_idf'
WORDCLOUD_PARAMS = {
    # 'font_path': '/system/Library/Fonts/ヒラギノ明朝 ProN.ttc',
    'font_path': '/home/ec2-user/.fonts/NotoSansCJKjp-Regular.otf',
    'background_color': 'white',
    'height': 400,
    'width': 600
}
# ELASTICSEARCH_URL = 'http://localhost:9200'
ELASTICSEARCH_URL = 'https://es.politylink.jp'

gql_client = GraphQLClient()
s3_client = boto3.client('s3')
es_client = ElasticsearchClient(ELASTICSEARCH_URL)


def to_date(dt):
    return date(year=dt.year, month=dt.month, day=dt.day)


def to_date_str(dt):
    return dt.strftime('%Y-%m-%d')


def fetch_tfidf(minutes, num_items=30):
    """
    call wordcloud server to fetch tfidf
    raise exception when request failed or response is empty
    """

    start_date = to_date(minutes.start_date_time)
    end_date = start_date + timedelta(days=1)
    request_param = {
        'committee': minutes.name,
        'start': to_date_str(start_date),
        'end': to_date_str(end_date),
        'items': num_items,
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


def fetch_tfidf2(minutes, num_items=30):
    """
    fetch tfidf from Elasticsearch
    """

    term2stats = es_client.get_term_statistics(minutes.id)
    tfidf = dict(map(lambda x: (x[0], x[1]['tfidf']), term2stats.items()))
    tfidf = {t: tfidf[t] for t in filter_by_pos(tfidf.keys(), WORDCLOUD_POS_TAGS)}
    tfidf = filter_dict_by_value(tfidf, num_items)
    return tfidf


def filter_dict_by_value(dict_, num_items):
    """
    valueが大きい上位のitemに絞る
    """

    sorted_items = sorted(dict_.items(), key=lambda x: x[1], reverse=True)
    num_items = min(len(dict_), num_items)
    return dict(sorted_items[:num_items])


def process(minutes):
    LOGGER.debug(f'process {minutes.id}')

    try:
        tfidf = fetch_tfidf2(minutes, num_items=30)
    except Exception:
        LOGGER.exception(f'failed to fetch tfidf')
        return

    id_ = minutes.id.split(':')[-1]
    local_path = f'./wordcloud/minutes/{id_}.jpg'
    s3_path = f'minutes/{id_}.jpg'

    wordcloud = WordCloud(**WORDCLOUD_PARAMS).generate_from_frequencies(tfidf)
    wordcloud.to_file(local_path)
    LOGGER.info(f'saved wordcloud to {local_path}')

    if args.publish:
        s3_client.upload_file(local_path, 'politylink', s3_path, ExtraArgs={'ContentType': 'image/jpeg'})
        gql_client.merge(Minutes({
            'id': minutes.id,
            'wordcloud': f'https://image.politylink.jp/{s3_path}'
        }))
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
