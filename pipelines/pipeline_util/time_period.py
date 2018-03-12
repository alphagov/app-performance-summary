"""
A helper class to represent time periods
"""
import datetime
from collections import namedtuple
from pipeline_util.pipeline_configuration import PipelineConfiguration

DateRange = namedtuple('DateRange', ('start_date', 'end_date',))


def previous_month():
    today = datetime.date.today()
    end_date = today.replace(day=1) - datetime.timedelta(days=1)
    start_date = end_date.replace(day=1)

    return DateRange(start_date=start_date, end_date=end_date)


def get_date_range_phrase(time_period):
    generator = StartEndDatePhraseGenerator()
    return generator.get_date_range_phrase(time_period.start_date, time_period.end_date)


class StartEndDatePhraseGenerator:
    PERIOD_TYPES = ['daily', 'weekly', 'monthly']

    def get_period_date_range_phrase(self, period_type, end_date=None):
        start_date = None
        if period_type == 'daily':
            start_date = end_date
        elif period_type == 'weekly':
            start_date = end_date - rd.relativedelta(weeks=1) + \
                         rd.relativedelta(days=1)
        else:
            # Assume monthly
            start_date = end_date - rd.relativedelta(months=1) + \
                         rd.relativedelta(days=1)

        date_range_phrase = self.get_date_range_phrase(start_date, end_date)
        return date_range_phrase

    def get_date_range_phrase(self, start_date, end_date):
        phrase = None
        start_date_phrase = \
            start_date.strftime(PipelineConfiguration.DATE_OUTPUT_FORMAT)
        if start_date == end_date:
            return start_date_phrase

        end_date_phrase = \
            end_date.strftime(PipelineConfiguration.DATE_OUTPUT_FORMAT)
        phrase = \
            start_date_phrase + '_' + end_date_phrase
        return phrase
