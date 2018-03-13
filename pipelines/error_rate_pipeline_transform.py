'''
Extract step for application error rates pipeline
'''
import luigi
from pipeline_steps.aggregate_error_rates import AggregateErrorRates
from pipeline_steps.drop_rows_with_missing_data import DropRowsWithMissingData
from error_rate_pipeline_extract import ErrorRatePipelineExtract
from base import BaseTask

class ErrorRatePipelineTransform(BaseTask):
    application = luigi.Parameter(
        default='whitehall-admin',
        always_in_help=True,
        description=("Application to extract error rates for")
    )

    def requires(self):
        return [
            ErrorRatePipelineExtract(application=self.application, date_interval=self.date_interval),
        ]

    def __init__(self, *args, **kwargs):
        super().__init__(task_name='error_rate_transform', *args, **kwargs)

        self.segment = self.application

    def output(self):
        return self.snapshot_target()

    def run(self):
        df = self.load_from_step('error_rate_extract', self.segment)

        aggregate_step = AggregateErrorRates()
        drop_missing_data_step = DropRowsWithMissingData()

        drop_missing_data_step.validate_input(
            df,
            df_name='input from error_rate_extract {}'.format(self.segment)
        )

        df = drop_missing_data_step.transform(df)
        self.save_snapshot(df, 'drop_rows_with_missing_data')

        drop_missing_data_step.validate_output(
            df,
            df_name='output from error_rates_transform step 1'
        )

        aggregate_step.validate_input(
            df,
            df_name='input from error_rates_transform step 1'
        )

        df = aggregate_step.transform(df, self.application, self.closed_date_range)
        self.save_snapshot(df, 'aggregate_error_rates')

        aggregate_step.validate_output(
            df,
            df_name='output from error_rates_transform step 2'
        )

        self.save_target_snapshot(df)


if __name__ == '__main__':
    luigi.run(main_task_cls=ErrorRatePipelineTransform, local_scheduler=True)
