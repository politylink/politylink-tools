import argparse
import logging

from politylink.graphql.client import GraphQLClient

LOGGER = logging.getLogger(__name__)


def main():
    gql_client = GraphQLClient()

    objects = []
    if args.bill:
        bills = gql_client.get_all_bills(['id', 'news'])
        LOGGER.info(f'fetched {len(bills)} bills to clean')
        objects += bills
    if args.minutes:
        minutesList = gql_client.get_all_minutes(['id', 'news'])
        LOGGER.info(f'fetched {len(minutesList)} minutes to clean')
        objects += minutesList
    LOGGER.info(f'registered {len(objects)} objects to clean')

    for obj in objects:
        news_ids = list(map(lambda x: x.id, obj.news))
        if news_ids:
            gql_client.bulk_unlink(news_ids, [obj.id] * len(news_ids))
            LOGGER.info(f'removed {len(news_ids)} news links from {obj.id}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Newsの紐付けを削除する')
    parser.add_argument('-b', '--bill', help='BillとNewsの紐付けを削除する', action='store_true')
    parser.add_argument('-m', '--minutes', help='MinutesとNewsの紐付けを削除する', action='store_true')
    parser.add_argument('-v', '--verbose', action='store_true')
    args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO)
    main()
