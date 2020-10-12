from datetime import datetime


def date_type(date_str):
    return datetime.strptime(date_str, '%Y-%m-%d').date()
