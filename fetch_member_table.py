import pandas as pd
import requests
from scrapy.http import HtmlResponse


def parse_table(table):
    records = []
    for row in table.xpath('./tr')[1:]:
        cells = row.xpath('./td')
        record = {
            'name': ''.join(cells[4].xpath('.//text()').get().split()),
            'website': row.xpath('.//a[@class="links weblink"]/@href').get(),
            'twitter': row.xpath('.//a[@class="links twitters"]/@href').get(),
            'facebook': row.xpath('.//a[@class="links facebooks"]/@href').get()
        }
        records.append(record)
    return records


def main():
    records = []

    url = 'https://democracy.minibird.jp/'
    response = HtmlResponse(url=url, body=requests.get(url).text, encoding='utf-8')
    table = response.xpath('//table[@id="table1"]')
    records += parse_table(table)

    url = 'https://democracy.minibird.jp/councillors.php'
    response = HtmlResponse(url=url, body=requests.get(url).text, encoding='utf-8')
    table = response.xpath('//table[@id="table2"]')
    records += parse_table(table)

    df = pd.DataFrame(records)
    df.to_csv('./data/member.csv', index=False)
    print('saved ./data/member.csv')


if __name__ == '__main__':
    main()
