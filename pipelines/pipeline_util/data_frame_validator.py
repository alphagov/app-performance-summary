import pandas as pd


class DataFrameValidator:
    """
    Supports common validation methods that are applied to data frames
    that are used or produced by pipeline steps.
    """

    def __init__(self, df_name):
        self.df_name = df_name
        self.row_number_ls = []

    def get_error_row_numbers(self):
        return self.row_number_ls

    def check_not_null(self, df):
        if df is None:
            raise ValueError('{} data frame is null'.format(
                             self.df_name))

    def check_not_empty(self, df):
        if df.empty:
            raise ValueError('{} data frame is empty'.format(
                             self.df_name))

    def check_data_frames_same_total_rows(self, df1, df2):
        if len(df1) != len(df2):
            raise ValueError('{} dataframes size changed'.format(self.df_name))

    def check_column_exists(self, df, col_name):
        if col_name not in df.columns:
            error_template = '{} data frame is missing the required column {}'
            raise ValueError(error_template.format(self.df_name, col_name))

    def check_no_empty_values(self, df, col_name):
        self.error_row_numbers = []
        tmp_df = pd.DataFrame(columns=[col_name])
        tmp_df[col_name] = df[col_name]
        tmp_df['row_number'] = range(0, len(tmp_df))
        error_df = tmp_df[tmp_df[col_name].isnull()]
        if not error_df.empty:
            self.row_number_ls = list(error_df['row_number'])
            error_messages = []
            error_template = ('{} Row {} Column \'{}\' is empty')
            for row_number in self.row_number_ls:
                error_message = error_template.format(
                    self.df_name,
                    row_number,
                    col_name)
                error_messages.append(error_message)

            all_error_messages = '\n'.join(error_messages)
            raise ValueError(all_error_messages)

    def check_column_not_exists(self, df, col_name):
        if col_name in df.columns:
            error_template = '{} data frame already has column {}'
            raise ValueError(error_template.format(self.df_name, col_name))

    def check_column_values_greater_than(self, df, col_name, lower_limit):
        self.error_row_numbers = []
        tmp_df = pd.DataFrame(columns=[col_name])
        tmp_df[col_name] = df[col_name]
        tmp_df['row_number'] = range(0, len(tmp_df))

        if lower_limit is None:
            error_template = ('No lower limit defined.')
            raise ValueError(error_template)

        error_df = tmp_df[tmp_df[col_name] <= lower_limit]

        if not error_df.empty:
            error_messages = []
            self.row_number_ls = list(error_df['row_number'])

            for row_number in self.row_number_ls:
                error_template = ('{} Row {} Column \'{}\' has a '
                                  'value {} which is less than or equal to {}')
                error_message = error_template.format(
                    self.df_name,
                    row_number,
                    col_name,
                    error_df[col_name][row_number],
                    lower_limit)
                error_messages.append(error_message)

            all_error_messages = '\n'.join(error_messages)
            raise ValueError(all_error_messages)

    def check_column_values_within_limits(self, df, col_name, lower_limit,
                                          upper_limit):
        self.error_row_numbers = []
        tmp_df = pd.DataFrame(columns=[col_name])
        tmp_df[col_name] = df[col_name]
        tmp_df['row_number'] = range(0, len(tmp_df))

        if (lower_limit is not None and upper_limit is not None):
            error_df = tmp_df[(tmp_df[col_name] < lower_limit) |
                              (tmp_df[col_name] > upper_limit)]
        elif lower_limit is not None:
            error_df = tmp_df[tmp_df[col_name] < lower_limit]
        elif upper_limit is not None:
            error_df = tmp_df[tmp_df[col_name] > upper_limit]
        else:
            error_template = ('Neither lower_limit nor upper_limit'
                              ' values have been defined.')
            raise ValueError(error_template)

        if not error_df.empty:
            self.row_number_ls = list(error_df['row_number'])
            error_messages = []
            for row_number in self.row_number_ls:
                error_template = ('{} Row {} Column \'{}\' has a '
                                  'value {} which is outside [{},{}]')
                error_message = error_template.format(
                    self.df_name,
                    row_number,
                    col_name,
                    error_df[col_name][row_number],
                    self.__format_boundary_value(lower_limit),
                    self.__format_boundary_value(upper_limit))
                error_messages.append(error_message)

            all_error_messages = '\n'.join(error_messages)
            raise ValueError(all_error_messages)

    def check_a_at_least_b(self, df, column_a, column_b):
        self.row_number_ls = []
        tmp_df = pd.DataFrame(columns=[column_a, column_b])
        tmp_df[column_a] = df[column_a]
        tmp_df[column_b] = df[column_b]
        tmp_df['diff'] = tmp_df[column_a] - tmp_df[column_b]
        tmp_df['row_number'] = range(0, len(tmp_df))
        error_df = tmp_df[tmp_df['diff'] < 0]

        if not error_df.empty:
            error_messages = []
            self.row_number_ls = list(error_df['row_number'])
            error_template = ('{} Row {} Column \'{}\' value \'{}\''
                              ' is greater than Column \'{}\' value \'{}\'')
            for row_number in self.row_number_ls:
                error_message = error_template.format(
                    self.df_name,
                    row_number,
                    column_b,
                    error_df[column_b][row_number],
                    column_a,
                    error_df[column_a][row_number])
                error_messages.append(error_message)

            all_error_messages = '\n'.join(error_messages)
            raise ValueError(all_error_messages)

    def check_item_in_list(self, df, col_name, all_valid_choices_list_name,
                           all_valid_choices):
        tmp_df = pd.DataFrame(columns=[col_name])
        tmp_df[col_name] = df[col_name]
        tmp_df['row_number'] = range(0, len(tmp_df))
        tmp_df['has_valid_choice'] = tmp_df[col_name].isin(all_valid_choices)
        error_df = tmp_df[~tmp_df['has_valid_choice']]

        if not error_df.empty:
            self.row_number_ls = list(error_df['row_number'])
            error_messages = []
            error_template = ("{} Row {} Column {} value '{}'"
                              " is not in {}")
            for row_number in self.row_number_ls:
                error_message = error_template.format(
                    self.df_name,
                    row_number,
                    col_name,
                    error_df[col_name][row_number],
                    all_valid_choices_list_name)
                error_messages.append(error_message)

            all_error_messages = '\n'.join(error_messages)
            raise ValueError(all_error_messages)

    def check_column_has_unique_values(self, df, col_name):
        difference = len(df[col_name]) - len(df[col_name].unique())
        if difference == 1:
            error_template = '{} column {} has 1 duplicate value'
            error_message = error_template.format(self.df_name, col_name)
            raise ValueError(error_message)
        elif difference > 1:
            error_template = '{} column {} has {} duplicate values'
            error_message = error_template.format(self.df_name, col_name,
                                                  difference)
            raise ValueError(error_message)

    def check_column_increases(self, df, col_name):
        if not pd.Index(df[col_name]).is_monotonic:
            error_template = '{} column {} is not in increasing order'
            error_message = error_template.format(self.df_name, col_name)

            raise ValueError(error_message)

    def __format_boundary_value(self, boundary_value):
        if boundary_value is None:
            return '?'
        else:
            return boundary_value
