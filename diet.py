import argparse
import logging
from datetime import datetime

import pandas as pd

from politylink.graphql.client import GraphQLClient
from politylink.graphql.schema import Diet, _Neo4jDateTimeInput
from politylink.idgen import idgen

LOGGER = logging.getLogger(__name__)


def to_neo4j_datetime(dt_str):
    dt = datetime.strptime(dt_str, '%Y-%m-%d').date()
    return _Neo4jDateTimeInput(year=dt.year, month=dt.month, day=dt.day)


def main(fp):
    gql_client = GraphQLClient()
    df = pd.read_csv(fp)

    diets = []
    for _, row in df.iterrows():
        diet = Diet(None)
        diet.number = int(row['number'])
        diet.name = f'第{diet.number}回国会'
        diet.category = row['category']
        diet.start_date = to_neo4j_datetime(row['start_date'])
        diet.end_date = to_neo4j_datetime(row['end_date'])
        diet.id = idgen(diet)
        diets.append(diet)

    gql_client.bulk_merge(diets)
    LOGGER.info(f'merged {len(diets)} diets')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Dietを手動で定義する')
    parser.add_argument('-f', '--file', default='./data/diet.csv')
    parser.add_argument('-v', '--verbose', action='store_true')
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO)
    logging.getLogger('sgqlc').setLevel(logging.INFO)
    main(args.file)
