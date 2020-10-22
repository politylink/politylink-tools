import argparse
import logging
import re

import pandas as pd

from politylink.graphql.client import GraphQLClient
from politylink.graphql.schema import Minutes, _Neo4jDateTimeInput, Url
from politylink.helpers import CommitteeFinder, MinutesFinder
from politylink.idgen import idgen

LOGGER = logging.getLogger(__name__)

client = GraphQLClient()
committee_finder = CommitteeFinder()
minutes_finder = MinutesFinder()


def get_next_meeting_number(diet_number, house_name, meeting_name):
    def extract_meeting_number(minutes_name):
        p = r'第([0-9]+)号'
        m = re.search(p, minutes_name)
        return int(m.group(1))

    def get_maximum_meeting_number(minutes_list):
        if not minutes_list:
            return 0
        return max(map(lambda x: extract_meeting_number(x.name), minutes_list))

    pattern = f'第{diet_number}回{house_name}{meeting_name}'
    minutes_list = minutes_finder.find(pattern)
    LOGGER.debug(f'found {len(minutes_list)} minutes for {pattern}')
    next_meeting_number = get_maximum_meeting_number(minutes_list) + 1
    LOGGER.debug(f'assigned {next_meeting_number} as next meeting number for {pattern}')
    return next_meeting_number


def build_minutes(diet_number, house_name, meeting_name, meeting_number, year, month, day):
    minutes = Minutes(None)
    minutes.name = f'第{diet_number}回{house_name}{meeting_name}第{meeting_number}号'
    minutes.start_date_time = _Neo4jDateTimeInput(year=year, month=month, day=day)
    minutes.id = idgen(minutes)
    return minutes


def build_tv_url(href):
    url = Url(None)
    url.url = href
    url.title = '審議中継'
    for domain in ['shugiintv.go.jp', 'sangiin.go.jp']:
        if domain in href:
            url.domain = domain
    url.id = idgen(url)
    return url


def main(fp):
    df = pd.read_csv(fp)
    LOGGER.info(f'loaded {len(df)} records from {fp}')

    # fill meeting number
    meeting_number_list = []
    for _, row in df.iterrows():
        meeting_number = row['meeting_number'] or \
                         get_next_meeting_number(row['diet_number'], row['house_name'], row['meeting_name'])
        meeting_number_list.append(meeting_number)
    df['meeting_number'] = meeting_number_list
    df.to_csv(fp, index=False)
    LOGGER.info(f'updated {fp}')

    # collect GraphQL arguments
    objects, from_ids, to_ids = [], [], []
    for _, row in df.iterrows():
        minutes = build_minutes(row['diet_number'], row['house_name'], row['meeting_name'], row['meeting_number'],
                                row['year'], row['month'], row['day'])
        objects.append(minutes)

        # link to committee
        committees = committee_finder.find(minutes.name)
        if len(committees) != 1:
            raise ValueError(f'found none/multiple committees for {minutes.name}')
        from_ids.append(minutes.id)
        to_ids.append(committees[0].id)

        # link to tv url if exists
        if row['tv']:
            url = build_tv_url(row['tv'])
            objects.append(url)
            from_ids.append(url.id)
            to_ids.append(minutes.id)

    client.bulk_merge(objects)
    LOGGER.info(f'merged {len(objects)} objects')
    client.bulk_link(from_ids, to_ids)
    LOGGER.info(f'linked {len(from_ids)} objects')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Minutesを手動で登録する')
    parser.add_argument('-f', '--file', default='./data/minutes.csv')
    parser.add_argument('-v', '--verbose', action='store_true')
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO)
    logging.getLogger('sgqlc').setLevel(logging.INFO)
    main(args.file)
