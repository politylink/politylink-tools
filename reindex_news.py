import argparse
import logging

from politylink.elasticsearch.client import ElasticsearchClient, OpType
from politylink.elasticsearch.schema import NewsText
from politylink.graphql.client import GraphQLClient
from utils import date_type

LOGGER = logging.getLogger(__name__)


def to_date_str(dt):
    return '{:02d}-{:02d}-{:02d}'.format(dt.year, dt.month, dt.day)


def main():
    gql_client = GraphQLClient()
    es_client = ElasticsearchClient()

    news_list = gql_client.get_all_news(fields=['id', 'published_at'],
                                        start_date=args.start_date, end_date=args.end_date)
    LOGGER.info(f'fetched {len(news_list)} news from GraphQL')

    if news_list:
        news_text_list = list(map(
            lambda news: NewsText({'id': news.id, 'date': to_date_str(news.published_at)}),
            news_list
        ))
        es_client.bulk_index(news_text_list, op_type=OpType.UPDATE)
        LOGGER.info(f're-indexed {len(news_text_list)} news text')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='ElasticsearchにNewsを登録し直す')
    parser.add_argument('-s', '--start_date', help='開始日（例: 2020-01-01）', type=date_type)
    parser.add_argument('-e', '--end_date', help='終了日（例: 2020-01-01）', type=date_type)
    parser.add_argument('-v', '--verbose', action='store_true')
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO)
    logging.getLogger('sgqlc').setLevel(logging.INFO)
    main()
