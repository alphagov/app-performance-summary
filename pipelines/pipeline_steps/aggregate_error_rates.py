import pandas as pd
import logging
from pipeline_util.data_frame_validator import DataFrameValidator
from pipeline_util.time_period import get_date_range_phrase

class AggregateErrorRates:
    """
    Sum up HTTP response counts over a time period, and calculate the percentages.
    """
    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def validate_input(self, data, df_name):
        validator = DataFrameValidator(df_name)
        validator.check_column_exists(data, 'timestamp')
        validator.check_column_exists(data, 'count_total')
        validator.check_column_exists(data, 'count_error')
        validator.check_no_empty_values(data, 'timestamp')
        validator.check_no_empty_values(data, 'count_total')
        validator.check_no_empty_values(data, 'count_error')
        validator.check_column_has_unique_values(data, 'timestamp')
        validator.check_column_increases(data, 'timestamp')
        validator.check_column_values_greater_than(data, 'count_error', -1)
        validator.check_a_at_least_b(data, 'count_total', 'count_error')

        self.logger.debug('Input is valid')

    def transform(self, data, app_name, reporting_period):
        sums = data.filter(items=['count_total', 'count_error']).sum()
        self.logger.info('Aggregated counts %s', sums)

        data = pd.DataFrame(
            [[
                app_name,
                get_date_range_phrase(reporting_period),
                sums.count_error,
                sums.count_total,
                100 - (sums.count_error * 100 / sums.count_total),
            ]],
            columns = ['application', 'reporting_period', 'count_error', 'count_total', 'success_rate']
        )

        return data

    def validate_output(self, data, df_name):
        validator = DataFrameValidator(df_name)
        validator.check_column_exists(data, 'application')
        validator.check_column_exists(data, 'reporting_period')
        validator.check_column_exists(data, 'count_total')
        validator.check_column_exists(data, 'count_error')
        validator.check_column_exists(data, 'success_rate')
        validator.check_column_values_within_limits(data, 'success_rate', 0, 100)

        self.logger.debug('Output is valid')
