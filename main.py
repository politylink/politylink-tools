import json
import logging

import requests
from politylink.elasticsearch.client import ElasticsearchClient, ElasticsearchClientException
from politylink.graphql.client import GraphQLClient
from tqdm import tqdm

LOGGER = logging.getLogger(__name__)
MINUTES_HANDLER = 'https://sharpspock.herokuapp.com/minutes'
BILLS_HANDLER = 'https://sharpspock.herokuapp.com/bills'


def fetch_all_news(gql_client):
    query = """
    {
        News {
            id
            title
            publishedAt{ year, month, day, hour, minute }
        }
    }
    """
    res = gql_client.exec(query)
    return res["News"]


def call_api(news, news_text, handler):
    text = " ".join([news_text.title, news_text.body])
    date = news["publishedAt"]
    date_str = "{}/{}/{} {}:{}".format(date["year"], date["month"], date["day"], date["hour"], date["minute"])
    json_data = json.dumps({"text": text, "date": date_str}, ensure_ascii=False)
    res = requests.post(handler,
                        data=json_data.encode("utf-8"),
                        headers={'Content-Type': 'application/json'})
    return res.json()


def fetch_matched_minutes(news, news_text):
    res = call_api(news, news_text, MINUTES_HANDLER)
    return res['minutes']


def fetch_matched_bills(news, news_text):
    res = call_api(news, news_text, BILLS_HANDLER)
    return res['bills']


def main():
    gql_client = GraphQLClient()
    es_client = ElasticsearchClient()

    news_list = fetch_all_news(gql_client)
    LOGGER.info(f'fetched {len(news_list)} news from GraphQL')

    for news in tqdm(news_list):
        try:
            news_text = es_client.get(news['id'])
        except ElasticsearchClientException as e:
            LOGGER.warning(e)
            continue
        minutes_list = fetch_matched_minutes(news, news_text)
        if minutes_list:
            LOGGER.info(f'found {len(minutes_list)} minutes for {news["id"]}')
            for minutes in minutes_list:
                gql_client.link(news["id"], minutes["id"])
        bill_list = fetch_matched_bills(news, news_text)
        if bill_list:
            LOGGER.info(f'found {len(bill_list)} bills for {news["id"]}')
            for bill in bill_list:
                gql_client.link(news["id"], bill["id"])


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    logging.getLogger('elasticsearch').setLevel(logging.WARNING)
    main()
