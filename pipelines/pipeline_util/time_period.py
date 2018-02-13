"""
A helper class to represent time periods
"""
import datetime
from collections import namedtuple

DateRange = namedtuple('DateRange', ('start_date', 'end_date',))


def previous_month():
    today = datetime.date.today()
    end_date = today.replace(day=1) - datetime.timedelta(days=1)
    start_date = end_date.replace(day=1)

    return DateRange(start_date=start_date, end_date=end_date)
