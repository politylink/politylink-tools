import argparse
import logging

import pandas as pd

from politylink.graphql.client import GraphQLClient

LOGGER = logging.getLogger(__name__)


def main(fp):
    gql_client = GraphQLClient()
    df = pd.read_csv(fp)

    gql_client.bulk_delete(ids=df['id'])
    LOGGER.info(f'deleted {len(df)} items')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='データを削除する')
    parser.add_argument('-f', '--file', default='./data/delete.csv')
    parser.add_argument('-v', '--verbose', action='store_true')
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO)
    logging.getLogger('sgqlc').setLevel(logging.INFO)
    main(args.file)
