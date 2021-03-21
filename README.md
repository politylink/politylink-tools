```
poetry run python news.py -s 2020-09-22 -e 2020-09-23 --skip_bill --skip_minutes
poetry run python timeline.py -s 2020-09-22 -e 2020-09-23
poetry run python alias.py
poetry run python minutes.py
poetry run python minutes_wordcloud.py -s 2020-12-01 -e 2020-12-08 --publish
poetry run python link.py
poetry run python link.py --file ./data/unlink.csv --delete
```