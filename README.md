```
poetry run python news.py -s 2020-09-22 -e 2020-09-23 --skip_bill --skip_minutes
poetry run python timeline.py -s 2020-09-22 -e 2020-09-23
poetry run python bill_alias.py
poetry run python minutes.py
poetry run python minutes_wordcloud.py -s 2020-12-01 -e 2020-12-08 --publish
poetry run python link.py
poetry run python link.py --file ./data/unlink.csv --delete
poetry run python find_news.py -q 国民 -s 2021-04-25 -e 2021-04-26
poetry run python reindex_news.py -s 2021-01-01 -e 2021-05-01
poetry run python elasitcsearch_syncer.py
```