import argparse
import logging
from datetime import datetime

from tqdm import tqdm

from politylink.graphql.client import GraphQLClient
from politylink.graphql.schema import _MinutesFilter, _Neo4jDateTimeInput, Minutes
from politylink.helpers import BillFinder
from utils import date_type

LOGGER = logging.getLogger(__name__)
GQL_CLIENT = GraphQLClient()
BILL_FINDER = BillFinder()


def fetch_all_minutes(start_date, end_date):
    def to_neo4j_date(dt):
        return _Neo4jDateTimeInput(year=dt.year, month=dt.month, day=dt.day)

    filter_ = _MinutesFilter(None)
    filter_.start_date_time_gte = to_neo4j_date(start_date)
    filter_.start_date_time_lt = to_neo4j_date(end_date)
    return GQL_CLIENT.get_all_minutes(filter_=filter_, fields=['id', 'topics', 'topic_ids', 'start_date_time'])


def get_topic_id(topic, date):
    """
    Copied from SpiderTemplate in politylink-crawler
    """
    try:
        bill = BILL_FINDER.find_one(text=topic, date=date)
        return bill.id
    except ValueError as e:
        LOGGER.debug(e)
        return ''


def reprocess_minutes(minutes):
    LOGGER.debug(f'process {minutes.id}')

    if minutes.topics:
        topic_ids = list(map(lambda x: get_topic_id(x, minutes.start_date_time), minutes.topics))
        if topic_ids != minutes.topic_ids:
            # need to create new instance to avoid neo4j datetime error
            updated_minutes = Minutes(None)
            updated_minutes.id = minutes.id
            updated_minutes.topic_ids = topic_ids
            GQL_CLIENT.merge(updated_minutes)
            LOGGER.debug(f'updated topic ids from {minutes.topic_ids} to {topic_ids}')

        bill_ids = list(filter(lambda x: x, topic_ids))
        if bill_ids:
            GQL_CLIENT.bulk_link([minutes.id] * len(bill_ids), bill_ids)
            LOGGER.debug(f'linked {len(bill_ids)} bills to {minutes.id}')


def main():
    minutes_list = fetch_all_minutes(args.start, args.end)
    LOGGER.info(f'fetched {len(minutes_list)} minutes')

    for minutes in tqdm(minutes_list):
        reprocess_minutes(minutes)
    LOGGER.info(f'processed {len(minutes_list)} minutes')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='MinutesとBillのリンクを再計算する')
    parser.add_argument('-v', '--verbose', action='store_true')
    parser.add_argument('-s', '--start', help='開始日（例: 2020-01-01）', type=date_type, default=datetime.today())
    parser.add_argument('-e', '--end', help='終了日（例: 2020-01-01）', type=date_type, default=datetime.today())
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO)
    logging.getLogger('sgqlc').setLevel(logging.INFO)
    main()
