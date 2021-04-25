import argparse
import json
import logging
import os
from datetime import date
from pathlib import Path

import boto3
import requests
from tqdm import tqdm
from wordcloud import WordCloud

from politylink.elasticsearch.client import ElasticsearchClient, ElasticsearchException
from politylink.graphql.client import GraphQLClient
from politylink.graphql.schema import Minutes
from politylink.nlp.utils import filter_by_pos, WORDCLOUD_POS_TAGS
from politylink.utils import filter_dict_by_value
from utils import date_type

LOGGER = logging.getLogger(__name__)
WORDCLOUD_SERVER = 'https://api.politylink.jp'
# WORDCLOUD_SERVER = 'http://localhost:5000'
WORDCLOUD_PARAMS = {
    # 'font_path': '/system/Library/Fonts/ヒラギノ明朝 ProN.ttc',
    'font_path': '/home/ec2-user/.fonts/NotoSansCJKjp-Regular.otf',
    'background_color': 'white',
    'height': 400,
    'width': 600
}

gql_client = GraphQLClient()
s3_client = boto3.client('s3')
es_client = ElasticsearchClient()


def fetch_term_statistics(minutes_id):
    """
    fetch term statistics from Elasticsearch and apply filtering for wordcloud
    for storage efficiency, stats are compressed to two value tuple: (tf, tfidf)
    """

    term2stats = dict()
    term2stats_raw = es_client.get_term_statistics(minutes_id)
    terms = filter_by_pos(term2stats_raw.keys(), WORDCLOUD_POS_TAGS)
    for term in terms:
        stats = term2stats_raw[term]
        if stats['tf'] > 1:
            term2stats[term] = (stats['tf'], round(stats['tfidf'], 2))
    return term2stats


def process(minutes_id, term2stats):
    LOGGER.debug(f'process {minutes_id}')

    tfidf = dict(map(lambda x: (x[0], x[1][1]), term2stats.items()))
    tfidf = filter_dict_by_value(tfidf, num_items=30)

    id_ = minutes_id.split(':')[-1]
    local_path = f'./wordcloud/minutes/{id_}.jpg'
    s3_path = f'minutes/{id_}.jpg'

    wordcloud = WordCloud(**WORDCLOUD_PARAMS).generate_from_frequencies(tfidf)
    wordcloud.to_file(local_path)
    LOGGER.info(f'saved wordcloud to {local_path}')

    if args.publish:
        s3_client.upload_file(local_path, 'politylink', s3_path, ExtraArgs={'ContentType': 'image/jpeg'})
        gql_client.merge(Minutes({
            'id': minutes_id,
            'wordcloud': f'https://image.politylink.jp/{s3_path}'
        }))
        LOGGER.info(f'published wordcloud to {s3_path}')


def is_target_minutes(minutes):
    def to_date(dt):
        return date(year=dt.year, month=dt.month, day=dt.day)

    if not minutes.ndl_min_id:
        return False
    minutes_dt = to_date(minutes.start_date_time)
    if not (args.start_date <= minutes_dt < args.end_date):
        return False
    return True


def load_all_data(json_fp):
    if os.path.exists(json_fp):
        with open(json_fp, 'r') as f:
            return json.load(f)
    else:
        LOGGER.warning(f'{json_fp} does not exist')
        return dict()


def save_all_data(all_data, json_fp):
    with open(json_fp, 'w') as f:
        json.dump(all_data, f, ensure_ascii=False)


def post_all_date(json_fp):
    requests.post(
        f'{WORDCLOUD_SERVER}/load',
        json.dumps({'file': str(Path(json_fp).resolve())}),
        headers={'Content-Type': 'application/json'}
    )


def main():
    minutes_list = gql_client.get_all_minutes(fields=['id', 'name', 'start_date_time', 'ndl_min_id'])
    LOGGER.info(f'loaded {len(minutes_list)} minutes from GraphQL')
    minutes_list = list(filter(lambda x: is_target_minutes(x), minutes_list))
    LOGGER.info(f'filtered {len(minutes_list)} target minutes')
    all_data = load_all_data(args.file)
    LOGGER.info(f'loaded {len(all_data)} data from {args.file}')

    for minutes in tqdm(minutes_list):
        try:
            term2stats = fetch_term_statistics(minutes.id)
        except ElasticsearchException:
            LOGGER.exception(f'failed to load term statistics from Elasticsearch for {minutes.id}')
            continue
        if not term2stats:
            LOGGER.warning(f'term statistic is empty for {minutes.id}')
            continue
        all_data[minutes.id] = term2stats
        process(minutes.id, term2stats)
    LOGGER.info(f'processed {len(minutes_list)} minutes')
    save_all_data(all_data, args.file)
    LOGGER.info(f'saved {len(all_data)} data to {args.file}')
    post_all_date(args.file)
    LOGGER.info(f'posted {args.file} to wordcloud server')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Minutesのワードクラウドを生成する')
    parser.add_argument('-s', '--start_date', help='開始日（例: 2020-01-01）', type=date_type, required=True)
    parser.add_argument('-e', '--end_date', help='終了日（例: 2020-01-02）', type=date_type, required=True)
    parser.add_argument('-f', '--file', help='ワードクラウドサーバー用に全てのtfidfを保存するjsonファイル。',
                        default='./wordcloud/minutes/tfidf.json')
    parser.add_argument('-p', '--publish', help='画像をS3にアップロードする', action='store_true')
    parser.add_argument('-v', '--verbose', action='store_true')
    args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO)
    logging.getLogger('sgqlc').setLevel(logging.WARNING)
    logging.getLogger('botocore').setLevel(logging.WARNING)
    logging.getLogger('s3transfer').setLevel(logging.WARNING)
    main()
