import argparse
import logging

import pandas as pd

from politylink.graphql.client import GraphQLClient
from politylink.helpers import BillFinder

LOGGER = logging.getLogger(__name__)
bill_finder = BillFinder()
client = GraphQLClient()


def main(fp):
    df = pd.read_csv(fp).fillna('')
    LOGGER.info(f'loaded {len(df)} records from {fp}')

    alias_columns = list(filter(lambda x: x.startswith('alias'), df.columns))
    tag_columns = list(filter(lambda x: x.startswith('tag'), df.columns))
    LOGGER.info(f'found {len(alias_columns)} alias columns: {alias_columns}')
    LOGGER.info(f'found {len(tag_columns)} tag columns: {tag_columns}')

    objects = []
    for _, row in df.iterrows():
        bills = bill_finder.find(row['billNumber'])
        if len(bills) != 1:
            LOGGER.warning(f'found {len(bills)} Bills for {row["billNumber"]}')
            continue
        bill = bills[0]
        bill.aliases = list(filter(lambda x: row[x], alias_columns))
        bill.tags = list(filter(lambda x: row[x], tag_columns))
        objects.append(bill)
    client.bulk_merge(objects)
    LOGGER.info(f'merged {len(objects)} bills')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Bill.alias, Bill.tagsを手動で登録する')
    parser.add_argument('-f', '--file', default='./data/alias.csv')
    parser.add_argument('-v', '--verbose', action='store_true')
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO)
    logging.getLogger('sgqlc').setLevel(logging.INFO)
    main(args.file)
