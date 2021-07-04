import argparse
import logging
import re

from sgqlc.operation import Operation
from tqdm import tqdm

from politylink.elasticsearch.client import ElasticsearchClient, OpType
from politylink.elasticsearch.schema import BillText, BillCategory, BillStatus
from politylink.graphql.client import GraphQLClient, Query
from politylink.graphql.schema import _BillFilter, Bill

LOGGER = logging.getLogger(__name__)
gql_client = GraphQLClient(url='https://graphql.politylink.jp')
es_client = ElasticsearchClient()

GQL_ROOT_FIELDS = ['id', 'name', 'bill_number', 'category', 'aliases', 'tags']
GQL_DATE_FIELDS = ['submitted_date', 'passed_representatives_committee_date', 'passed_representatives_date',
                   'passed_councilors_committee_date', 'passed_councilors_date', 'proclaimed_date']
GQL_ES_FIELD_MAP = {
    Bill.id: BillText.Field.ID,
    Bill.bill_number: BillText.Field.BILL_NUMBER,
    Bill.name: BillText.Field.TITLE,
    Bill.tags: BillText.Field.TAGS,
    Bill.aliases: BillText.Field.ALIASES
}


def fetch_bill(bill_id):
    op = Operation(Query)
    bills = op.bill(filter=_BillFilter({'id': bill_id}))

    for field in GQL_ROOT_FIELDS + GQL_DATE_FIELDS:
        getattr(bills, field)()

    diets = bills.belonged_to_diets()
    diets.number()

    minutes = bills.be_discussed_by_minutes()
    minutes.start_date_time()

    res = gql_client.endpoint(op)
    data = (op + res).bill
    return data[0]


def to_date_str(dt):
    return '{:02d}-{:02d}-{:02d}'.format(dt.year, dt.month, dt.day)


def extract_diet_number(bill_number):
    pattern = r'第([0-9]+)回'
    m = re.search(pattern, bill_number)
    return int(m.group(1))


def calc_last_updated_date(bill):
    last_updated_date = ''
    for field in GQL_DATE_FIELDS:
        if getattr(bill, field).formatted:
            last_updated_date = max(last_updated_date, to_date_str(getattr(bill, field)))
    for minutes in bill.be_discussed_by_minutes:
        last_updated_date = max(last_updated_date, to_date_str(minutes.start_date_time))
    return last_updated_date


def build_bill_text(bill: Bill):
    bill_text = BillText(None)

    for gql_field, es_field in GQL_ES_FIELD_MAP.items():
        maybe_value = getattr(bill, gql_field.name, None)
        if maybe_value:  # ignore None or empty
            bill_text.set(es_field, maybe_value)

    bill_text.set(BillText.Field.CATEGORY, BillCategory.from_gql(bill))
    if bill.submitted_date.formatted:
        bill_text.set(BillText.Field.SUBMITTED_DATE, to_date_str(bill.submitted_date))
        bill_text.set(BillText.Field.LAST_UPDATED_DATE, calc_last_updated_date(bill))
        bill_text.set(BillText.Field.STATUS, BillStatus.from_gql(bill).index)
    bill_text.set(BillText.Field.SUBMITTED_DIET, extract_diet_number(bill.bill_number))
    if bill.belonged_to_diets:
        bill_text.set(BillText.Field.BELONGED_TO_DIETS, [diet.number for diet in bill.belonged_to_diets])
    return bill_text


def main():
    bills = gql_client.get_all_bills(fields=['id'])
    LOGGER.info(f'fetched {len(bills)} bills from GraphQL')

    sync_count = 0
    for bill in tqdm(bills):
        bill = fetch_bill(bill.id)
        if es_client.exists(bill.id):
            bill_text = build_bill_text(bill)
            es_client.index(bill_text, op_type=OpType.UPDATE)
            LOGGER.debug(f'synced {bill.id}')
            sync_count += 1
    LOGGER.info(f'synced {sync_count}/{len(bills)} bills to Elasticsearch')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='GraphQLのメタデータをElasticsearchに同期する')
    parser.add_argument('-v', '--verbose', action='store_true')
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO)
    logging.getLogger('sgqlc').setLevel(logging.INFO)
    logging.getLogger('elasticsearch').setLevel(logging.WARNING)
    main()
