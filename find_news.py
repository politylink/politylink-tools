import argparse
import logging

from politylink.elasticsearch.client import ElasticsearchClient
from politylink.elasticsearch.schema import NewsText
from politylink.graphql.client import GraphQLClient, GraphQLException

LOGGER = logging.getLogger(__name__)


def main(query='', bill_id_body=None):
    es_client = ElasticsearchClient(url='http://localhost:9201')
    gql_client = GraphQLClient(url='https://graphql.politylink.jp/')

    if bill_id_body:
        bill = gql_client.get(f'Bill:{bill_id_body}')
        query += bill.name

    news_texts = es_client.search(NewsText, query)
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
    parser = argparse.ArgumentParser(description='Newsを手動で探すための')
    parser.add_argument('-q', '--query', default='')
    parser.add_argument('-b', '--bill', help='Bill IDのBody')
    parser.add_argument('-v', '--verbose', action='store_true')
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO)
    logging.getLogger('sgqlc').setLevel(logging.INFO)
    main(args.query, args.bill)
