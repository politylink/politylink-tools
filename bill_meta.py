import argparse
import logging

import pandas as pd

from politylink.graphql.client import GraphQLClient
from politylink.graphql.schema import Bill
from politylink.helpers import BillFinder

LOGGER = logging.getLogger(__name__)
bill_finder = BillFinder()
client = GraphQLClient()


def main(fp):
    df = pd.read_csv(fp)
    LOGGER.info(f'loaded {len(df)} records from {fp}')

    bills = []
    for bill_number, df in df.groupby('bill'):
        try:
            bill = bill_finder.find_one(bill_number)
        except Exception as e:
            LOGGER.warning(f'found {len(bills)} Bills for {bill_number}', e)
            continue

        tags, aliases = [], []
        for _, row in df.iterrows():
            if row['key'] == 'TAG':
                tags.append(row['value'])
            elif row['key'] == 'ALIAS':
                aliases.append(row['value'])
        LOGGER.debug(f'found {len(tags)} tags and {len(aliases)} aliases for {bill.bill_number}')

        bills.append(Bill({'id': bill.id, 'tags': tags, 'aliases': aliases}))

    client.bulk_merge(bills)
    LOGGER.info(f'merged {len(bills)} bills')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='法律案のメタデータを手動で登録する')
    parser.add_argument('-f', '--file', default='./data/bill_meta.csv')
    parser.add_argument('-v', '--verbose', action='store_true')
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO)
    logging.getLogger('sgqlc').setLevel(logging.INFO)
    main(args.file)
