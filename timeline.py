import argparse
import logging
from collections import defaultdict
from datetime import timedelta, datetime

from tqdm import tqdm

from politylink.graphql.client import GraphQLClient
from politylink.graphql.schema import Timeline, _Neo4jDateTimeInput
from politylink.idgen import idgen
from utils import date_type

LOGGER = logging.getLogger(__name__)

BILL_DATE_FIELDS = [
    'submitted_date',
    'passed_representatives_committee_date',
    'passed_representatives_date',
    'passed_councilors_committee_date',
    'passed_councilors_date'
]
MINUTES_DATE_FIELD = [
    'start_date_time'
]
NEWS_DATE_FIELD = [
    'published_at'
]


def build_date_dict(object_list, date_fields):
    date2obj = defaultdict(list)
    for obj in object_list:
        for field in date_fields:
            date_dct = getattr(obj, field)
            if date_dct['year'] and date_dct['month'] and date_dct['day']:
                date = datetime(year=date_dct['year'], month=date_dct['month'], day=date_dct['day']).date()
                date2obj[date].append(obj)
    return date2obj


def main():
    gql_client = GraphQLClient()
    bill_list = gql_client.get_all_bills(['id'] + BILL_DATE_FIELDS)
    LOGGER.info(f'fetched {len(bill_list)} bills')
    minutes_list = gql_client.get_all_minutes(['id'] + MINUTES_DATE_FIELD)
    LOGGER.info(f'fetched {len(minutes_list)} minutes')
    news_list = gql_client.get_all_news(['id', 'is_timeline'] + NEWS_DATE_FIELD, args.start_date, args.end_date)
    LOGGER.info(f'fetched {len(news_list)} news')
    date2bill = build_date_dict(bill_list, BILL_DATE_FIELDS)
    date2minutes = build_date_dict(minutes_list, MINUTES_DATE_FIELD)
    date2news = build_date_dict(news_list, NEWS_DATE_FIELD)
    LOGGER.info(len(date2bill))

    dates = [args.start_date + timedelta(i) for i in range((args.end_date - args.start_date).days)]
    for date in tqdm(dates):
        timeline = Timeline(None)
        timeline.date = _Neo4jDateTimeInput(year=date.year, month=date.month, day=date.day)
        timeline.id = idgen(timeline)
        gql_client.merge(timeline)

        from_ids = []
        for bill in date2bill[date]:
            from_ids.append(bill.id)
        for minutes in date2minutes[date]:
            from_ids.append(minutes.id)
        for news in date2news[date]:
            if news.is_timeline:
                from_ids.append(news.id)
        gql_client.bulk_link(from_ids, [timeline.id] * len(from_ids))
        LOGGER.info(f'linked {len(from_ids)} events to {date}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Timelineを作成する')
    parser.add_argument('-s', '--start_date', help='開始日（例: 2020-01-01）', required=True, type=date_type)
    parser.add_argument('-e', '--end_date', help='終了日（例: 2020-01-01）', required=True, type=date_type)
    parser.add_argument('-v', '--verbose', action='store_true')
    args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO)
    main()
