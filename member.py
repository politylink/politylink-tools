import argparse
import logging

import pandas as pd

from politylink.graphql.client import GraphQLClient
from politylink.graphql.schema import Member
from politylink.helpers import MemberFinder

LOGGER = logging.getLogger(__name__)


def main(fp):
    client = GraphQLClient()
    member_finder = MemberFinder(search_fields=['name'])

    df = pd.read_csv(fp)
    LOGGER.info(f'load {len(df)} members from {fp}')

    members = []
    for _, row in df.iterrows():
        member = Member(None)
        for col in df.columns:
            setattr(member, col, row[col])
        try:
            member_ = member_finder.find_one(member.name)
            member.id = member_.id
        except ValueError:
            LOGGER.warning(f'{member.name} does not exist')
        else:
            members.append(member)
    client.bulk_merge(members)
    LOGGER.info(f'updated {len(members)} members')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='MemberのデータをCSVから追加する')
    parser.add_argument('-f', '--file', default='./data/member.csv')
    parser.add_argument('-v', '--verbose', action='store_true')
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO)
    logging.getLogger('sgqlc').setLevel(logging.INFO)
    main(args.file)
