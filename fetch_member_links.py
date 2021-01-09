import argparse
import logging

import pandas as pd
import requests
from scrapy.http import HtmlResponse

LOGGER = logging.getLogger(__name__)


def scrape_table(table):
    records = []
    for row in table.xpath('./tr')[1:]:
        cells = row.xpath('./td')
        record = {
            'name': ''.join(cells[4].xpath('.//text()').get().split()),
            'name_hira': ''.join(cells[4].xpath('./a/span[@class="ruby"]/text()').get().split()),
            'website': row.xpath('.//a[@class="links weblink"]/@href').get(),
            'twitter': row.xpath('.//a[@class="links twitters"]/@href').get(),
            'facebook': row.xpath('.//a[@class="links facebooks"]/@href').get()
        }
        records.append(record)
    LOGGER.debug(f'scraped {len(records)} records from table')
    return records


def main(fp):
    records = []

    url = 'https://democracy.minibird.jp/'
    response = HtmlResponse(url=url, body=requests.get(url).text, encoding='utf-8')
    table = response.xpath('//table[@id="table1"]')
    records += scrape_table(table)

    url = 'https://democracy.minibird.jp/councillors.php'
    response = HtmlResponse(url=url, body=requests.get(url).text, encoding='utf-8')
    table = response.xpath('//table[@id="table2"]')
    records += scrape_table(table)

    df = pd.DataFrame(records)
    df.to_csv(fp, index=False)
    LOGGER.info(f'saved {len(records)} records to {fp}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Memberのリンクを取得する')
    parser.add_argument('-f', '--file', default='./data/member.csv')
    parser.add_argument('-v', '--verbose', action='store_true')
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO)
    logging.getLogger('sgqlc').setLevel(logging.INFO)
    main(args.file)
