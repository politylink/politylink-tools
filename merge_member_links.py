import argparse
import logging

import pandas as pd

from politylink.graphql.client import GraphQLClient
from politylink.helpers import MemberFinder

LOGGER = logging.getLogger(__name__)


def main(fp):
    client = GraphQLClient()
    member_finder = MemberFinder(search_fields=['name', 'name_hira'])

    df = pd.read_csv(fp).fillna('')
    LOGGER.info(f'load {len(df)} members from {fp}')

    members = []
    for _, row in df.iterrows():
        member = None
        for search_field in ['name', 'name_hira']:
            try:
                member = member_finder.find_one(row[search_field], exact_match=True)
                break
            except ValueError as e:
                LOGGER.debug(e)
        if not member:
            LOGGER.warning(f'failed to find member for row={row}')
            continue
        for link_field in ['website', 'twitter', 'facebook']:
            if row[link_field]:
                setattr(member, link_field, row[link_field])
        members.append(member)
    client.bulk_merge(members)
    LOGGER.info(f'merged {len(members)} member links')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='MemberのリンクをCSVからGraphQLに追加する')
    parser.add_argument('-f', '--file', default='./data/member.csv')
    parser.add_argument('-v', '--verbose', action='store_true')
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO)
    logging.getLogger('sgqlc').setLevel(logging.INFO)
    main(args.file)
