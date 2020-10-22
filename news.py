import argparse
import json
import logging
from collections import defaultdict

import requests
from tqdm import tqdm

from politylink.elasticsearch.client import ElasticsearchClient
from politylink.graphql.client import GraphQLClient
from politylink.graphql.schema import News
from utils import date_type

LOGGER = logging.getLogger(__name__)
MINUTES_HANDLER = 'https://sharpspock.herokuapp.com/minutes'
BILLS_HANDLER = 'https://sharpspock.herokuapp.com/bills'
DIET_HANDLER = 'https://sharpspock.herokuapp.com/process'


def call_api(news, news_text, handler):
    text = ' '.join([news_text.title, news_text.body])
    date = news.published_at
    date_str = '{}/{}/{} {}:{}'.format(date.year, date.month, date.day, date.hour, date.minute)
    json_data = json.dumps({"text": text, "date": date_str}, ensure_ascii=False)
    res = requests.post(handler,
                        data=json_data.encode("utf-8"),
                        headers={'Content-Type': 'application/json'})
    return res.json()


def fetch_matched_minutes(news, news_text):
    res = call_api(news, news_text, MINUTES_HANDLER)
    return res['minutes']


def fetch_matched_bills(news, news_text):
    res = call_api(news, news_text, BILLS_HANDLER)
    return res['bills']


def fetch_is_timeline(news, news_text):
    res = call_api(news, news_text, DIET_HANDLER)
    return res['diet_flag'] > 0


def main():
    gql_client = GraphQLClient()
    es_client = ElasticsearchClient()

    news_list = gql_client.get_all_news(['id', 'title', 'published_at'], args.start_date, args.end_date)
    LOGGER.info(f'fetched {len(news_list)} news from GraphQL')

    stats = defaultdict(int)
    for news in tqdm(news_list):
        LOGGER.info(f'process {news.id}')
        stats['process'] += 1
        try:
            news_text = es_client.get(news.id)
            if not args.skip_minutes:
                LOGGER.debug(f'check Minutes for {news.id}')
                minutes_list = fetch_matched_minutes(news, news_text)
                if minutes_list:
                    gql_client.bulk_link([news.id] * len(minutes_list), map(lambda x: x['id'], minutes_list))
                    LOGGER.info(f'linked {len(minutes_list)} minutes for {news.id}')
            if not args.skip_bill:
                LOGGER.debug(f'check Bill for {news.id}')
                bill_list = fetch_matched_bills(news, news_text)
                if bill_list:
                    gql_client.bulk_link([news.id] * len(bill_list), map(lambda x: x['id'], bill_list))
                    LOGGER.info(f'linked {len(bill_list)} bills for {news.id}')
            if not args.skip_timeline:
                LOGGER.debug(f'check Timeline for {news.id}')
                is_timeline = fetch_is_timeline(news, news_text)
                if is_timeline:
                    # need to create new instance to avoid neo4j datetime error
                    updated_news = News(None)
                    updated_news.id = news.id
                    updated_news.is_timeline = is_timeline
                    gql_client.merge(updated_news)
                    LOGGER.info(f'linked {news.id} to timeline')
        except Exception as e:
            stats['fail'] += 1
            if isinstance(e, json.decoder.JSONDecodeError):
                LOGGER.warning(f'failed to parse API response for {news.id}')
            else:
                LOGGER.exception(f'failed to process {news.id}')
    LOGGER.info('processed {} news ({} success, {} fail)'.format(
        stats['process'], stats['process'] - stats['fail'], stats['fail']
    ))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='NewsをBillとMinutesに紐づける')
    parser.add_argument('-s', '--start_date', help='開始日（例: 2020-01-01）', type=date_type)
    parser.add_argument('-e', '--end_date', help='終了日（例: 2020-01-01）', type=date_type)
    parser.add_argument('-b', '--skip_bill', help='Billを関連付けない', action='store_true')
    parser.add_argument('-m', '--skip_minutes', help='Billを関連付けない', action='store_true')
    parser.add_argument('-t', '--skip_timeline', help='Timelineを関連付けない', action='store_true')
    parser.add_argument('-v', '--verbose', action='store_true')
    args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO)
    logging.getLogger('elasticsearch').setLevel(logging.WARNING)
    main()
