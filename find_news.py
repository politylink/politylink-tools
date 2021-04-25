import argparse
import logging

from politylink.elasticsearch.client import ElasticsearchClient
from politylink.elasticsearch.schema import NewsText
from politylink.graphql.client import GraphQLClient, GraphQLException

LOGGER = logging.getLogger(__name__)


def main():
    es_client = ElasticsearchClient()
    gql_client = GraphQLClient()

    query = args.query
    if args.bill:
        bill = gql_client.get(f'Bill:{args.bill}')
        query += bill.name

    news_texts = es_client.search(NewsText, query=query, start_date_str=args.start, end_date_str=args.end)
    for news_text in news_texts:
        try:
            news = gql_client.get(news_text.id)
        except GraphQLException as e:
            LOGGER.warning(e)
            continue
        print(news.id)
        print(news.publisher + '@' + news.published_at.formatted)
        print(news.title)
        print(news_text.body[:100])
        print(news.url)
        print()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Newsを手動で探す')
    parser.add_argument('-q', '--query', default='')
    parser.add_argument('-b', '--bill', help='Bill IDのBody')
    parser.add_argument('-v', '--verbose', action='store_true')
    parser.add_argument('-s', '--start', help='開始日（例: 2020-01-01）', default=None)
    parser.add_argument('-e', '--end', help='終了日（例: 2020-01-01）', default=None)
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO)
    logging.getLogger('sgqlc').setLevel(logging.INFO)
    main()
