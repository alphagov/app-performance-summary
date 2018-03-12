import logging
from pipeline_util.data_frame_validator import DataFrameValidator

class DropRowsWithMissingData:
    """
    Takes a time series and drops values where any column is missing
    """
    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def validate_input(self, data, df_name):
        validator = DataFrameValidator(df_name)
        validator.check_not_null(data)
        validator.check_not_empty(data)
        validator.check_column_exists(data, 'timestamp')
        validator.check_no_empty_values(data, 'timestamp')
        self.logger.debug('Input is valid')

    def transform(self, data):
        before = len(data)
        data = data.dropna()
        after = len(data)
        self.logger.info('Dropped %d rows with missing values', before-after)
        return data

    def validate_output(self, data, df_name):
        validator = DataFrameValidator(df_name)
        validator.check_column_exists(data, 'timestamp')
        validator.check_no_empty_values(data, 'timestamp')

        for col in data.columns.tolist():
            validator.check_no_empty_values(data, col)

        self.logger.debug('Output is valid')
