import argparse
import logging
from urllib.parse import urlparse

import pandas as pd

from politylink.graphql.client import GraphQLClient
from politylink.graphql.schema import Url
from politylink.helpers import BillFinder
from politylink.idgen import idgen

LOGGER = logging.getLogger(__name__)
bill_finder = BillFinder()
client = GraphQLClient()


def build_url(url, title):
    domain = urlparse(url).netloc.replace('www.', '')
    url = Url({'url': url, 'title': title, 'domain': domain})
    url.id = idgen(url)
    return url


def main(fp):
    df = pd.read_csv(fp).fillna('')
    LOGGER.info(f'loaded {len(df)} records from {fp}')

    urls, from_ids, to_ids = [], [], []
    for _, row in df.iterrows():
        try:
            bill = bill_finder.find_one(row['bill'])
        except Exception as e:
            LOGGER.warning(e)
            continue
        url = build_url(row['url'], row['title'])
        urls.append(url)
        from_ids.append(url.id)
        to_ids.append(bill.id)
    LOGGER.info(f'parse {len(urls)} urls')
    client.bulk_merge(urls)
    client.bulk_link(from_ids, to_ids)
    LOGGER.info(f'linked {len(urls)} urls')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Billの概要資料を手動で登録する')
    parser.add_argument('-f', '--file', default='./data/bill_url.csv')
    parser.add_argument('-v', '--verbose', action='store_true')
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO)
    logging.getLogger('sgqlc').setLevel(logging.INFO)
    main(args.file)
