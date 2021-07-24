import argparse
import logging
import re

from sgqlc.operation import Operation
from tqdm import tqdm

from politylink.elasticsearch.client import ElasticsearchClient, OpType
from politylink.elasticsearch.schema import BillText, BillCategory, BillStatus, ParliamentaryGroup, MemberText
from politylink.graphql.client import GraphQLClient, Query
from politylink.graphql.schema import _BillFilter, Bill, Member, _MemberFilter

LOGGER = logging.getLogger(__name__)
gql_client = GraphQLClient(url='https://graphql.politylink.jp')
es_client = ElasticsearchClient()


class ElasticsearchSyncer:
    def __init__(self, gql_client: GraphQLClient, es_client: ElasticsearchClient):
        self.gql_client = gql_client
        self.es_client = es_client
        self.sync_count = 0

    def sync(self, id_):
        """
        GraphQLのデータをElasticsearchに同期する
        """

        if not self.es_client.exists(id_):
            LOGGER.debug(f'skipped {id_}')
            return False
        gql_obj = self.fetch(id_)
        es_obj = self.convert(gql_obj)
        self.es_client.index(es_obj, op_type=OpType.UPDATE)
        LOGGER.debug(f'synced {id_}')
        self.sync_count += 1
        return True

    def fetch(self, id_):
        """
        GraphQLから必要なFieldを取得する
        """
        NotImplemented

    def convert(self, gql_obj):
        """
        GraphQLインスタンスをElasticsearchインスタンスに変換する
        """
        NotImplemented


class BillSyncer(ElasticsearchSyncer):
    GQL_ROOT_FIELDS = ['id', 'name', 'bill_number', 'category', 'aliases', 'tags', 'supported_groups', 'opposed_groups']
    GQL_DATE_FIELDS = ['submitted_date', 'passed_representatives_committee_date', 'passed_representatives_date',
                       'passed_councilors_committee_date', 'passed_councilors_date', 'proclaimed_date']
    GQL_ES_FIELD_MAP = {
        Bill.id: BillText.Field.ID,
        Bill.bill_number: BillText.Field.BILL_NUMBER,
        Bill.name: BillText.Field.TITLE,
        Bill.tags: BillText.Field.TAGS,
        Bill.aliases: BillText.Field.ALIASES
    }

    def fetch(self, bill_id) -> Bill:
        op = Operation(Query)
        bills = op.bill(filter=_BillFilter({'id': bill_id}))

        for field in self.GQL_ROOT_FIELDS + self.GQL_DATE_FIELDS:
            getattr(bills, field)()

        diets = bills.belonged_to_diets()
        diets.number()

        minutes = bills.be_discussed_by_minutes()
        minutes.start_date_time()

        members = bills.be_submitted_by_members()
        members.group()

        res = self.gql_client.endpoint(op)
        data = (op + res).bill
        return data[0]

    def convert(self, bill: Bill) -> BillText:
        bill_text = BillText()

        for gql_field, es_field in self.GQL_ES_FIELD_MAP.items():
            maybe_value = getattr(bill, gql_field.name, None)
            if maybe_value:  # ignore None or empty
                bill_text.set(es_field, maybe_value)

        bill_text.set(BillText.Field.CATEGORY, BillCategory.from_gql(bill).index)
        if bill.submitted_date.formatted:
            bill_text.set(BillText.Field.SUBMITTED_DATE, to_date_str(bill.submitted_date))
            bill_text.set(BillText.Field.LAST_UPDATED_DATE, self._calc_last_updated_date(bill))
            bill_text.set(BillText.Field.STATUS, BillStatus.from_gql(bill).index)
        bill_text.set(BillText.Field.SUBMITTED_DIET, extract_diet_number(bill.bill_number))
        if bill.belonged_to_diets:
            bill_text.set(BillText.Field.BELONGED_TO_DIETS, [diet.number for diet in bill.belonged_to_diets])

        field_list = [BillText.Field.SUBMITTED_GROUPS, BillText.Field.SUPPORTED_GROUPS, BillText.Field.OPPOSED_GROUPS]
        groups_list = [
            to_es_groups([member.group for member in bill.be_submitted_by_members if member.group]),
            to_es_groups(bill.supported_groups),
            to_es_groups(bill.opposed_groups)
        ]
        for field, groups in zip(field_list, groups_list):
            if groups:
                bill_text.set(field, groups)
        return bill_text

    def _calc_last_updated_date(self, bill: Bill) -> str:
        last_updated_date = ''
        for field in self.GQL_DATE_FIELDS:
            if getattr(bill, field).formatted:
                last_updated_date = max(last_updated_date, to_date_str(getattr(bill, field)))
        for minutes in bill.be_discussed_by_minutes:
            last_updated_date = max(last_updated_date, to_date_str(minutes.start_date_time))
        return last_updated_date


class MemberSyncer(ElasticsearchSyncer):
    GQL_FIELDS = ['id', 'name', 'name_hira', 'group']
    GQL_ES_FIELD_MAP = {
        Member.id: MemberText.Field.ID,
        Member.name: MemberText.Field.NAME,
        Member.name_hira: MemberText.Field.NAME_HIRA
    }

    def fetch(self, member_id) -> Member:
        op = Operation(Query)
        members = op.member(filter=_MemberFilter({'id': member_id}))

        for field in self.GQL_FIELDS:
            getattr(members, field)()

        # TODO: fetch only the latest activity once POL-344 is fixed
        activities = members.activities()
        activities.datetime()

        res = self.gql_client.endpoint(op)
        data = (op + res).member
        return data[0]

    def convert(self, member: Member) -> MemberText:
        member_text = MemberText()

        for gql_field, es_field in self.GQL_ES_FIELD_MAP.items():
            maybe_value = getattr(member, gql_field.name, None)
            if maybe_value:  # ignore None or empty
                member_text.set(es_field, maybe_value)

        if member.group:
            member_text.set(MemberText.Field.GROUP, ParliamentaryGroup.from_gql(member.group).index)

        if member.activities:
            member_text.set(MemberText.Field.LAST_UPDATED_DATE, self._calc_last_updated_date(member))

        return member_text

    def _calc_last_updated_date(self, member: Member) -> str:
        if member.activities:
            return max([to_date_str(activity.datetime) for activity in member.activities])
        return ''


def to_date_str(dt):
    return '{:02d}-{:02d}-{:02d}'.format(dt.year, dt.month, dt.day)


def to_es_groups(gql_groups):
    if not gql_groups:
        return list()
    return [ParliamentaryGroup.from_gql(gql_group).index for gql_group in gql_groups if gql_group]


def extract_diet_number(bill_number):
    pattern = r'第([0-9]+)回'
    m = re.search(pattern, bill_number)
    return int(m.group(1))


def main_bill():
    bills = gql_client.get_all_bills(fields=['id'])
    LOGGER.info(f'fetched {len(bills)} bills from GraphQL')

    bill_syncer = BillSyncer(gql_client, es_client)
    for bill in tqdm(bills):
        bill_syncer.sync(bill.id)
    LOGGER.info(f'synced {bill_syncer.sync_count}/{len(bills)} bills to Elasticsearch')


def main_member():
    members = gql_client.get_all_members(fields=['id'])
    LOGGER.info(f'fetched {len(members)} members from GraphQL')

    member_syncer = MemberSyncer(gql_client, es_client)
    for bill in tqdm(members):
        member_syncer.sync(bill.id)
    LOGGER.info(f'synced {member_syncer.sync_count}/{len(members)} members to Elasticsearch')


def main():
    if args.bill:
        main_bill()
    if args.member:
        main_member()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='GraphQLのメタデータをElasticsearchに同期する')
    parser.add_argument('-b', '--bill', help='Billを同期する', action='store_true')
    parser.add_argument('-m', '--member', help='Memberを同期する', action='store_true')
    parser.add_argument('-v', '--verbose', action='store_true')
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO)
    logging.getLogger('sgqlc').setLevel(logging.INFO)
    logging.getLogger('elasticsearch').setLevel(logging.WARNING)
    main()
