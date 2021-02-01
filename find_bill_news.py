import argparse
import logging

from politylink.elasticsearch.client import ElasticsearchClient
from politylink.elasticsearch.schema import NewsText
from politylink.graphql.client import GraphQLClient


def main(bill_id_body):
    es_client = ElasticsearchClient(url='http://localhost:9201')
    gql_client = GraphQLClient(url='https://graphql.politylink.jp/')

    bill = gql_client.get(f'Bill:{bill_id_body}')
    for news_text in es_client.search(NewsText, bill.name):
        news = gql_client.get(news_text.id)
        print(news.id)
        print(news.publisher + '@' + news.published_at.formatted)
        print(news.title)
        print(news_text.body[:100])
        print(news.url)
        print()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Linkを手動で定義する')
    parser.add_argument('-b', '--bill', help='Bill ID')
    parser.add_argument('-v', '--verbose', action='store_true')
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO)
    logging.getLogger('sgqlc').setLevel(logging.INFO)
    main(args.bill)
