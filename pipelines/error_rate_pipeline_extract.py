'''
Extract step for application error rates pipeline
'''
import luigi
from pipeline_steps.production_error_rates_source import ProductionErrorRatesSource
from base import BaseTask

class ErrorRatePipelineExtract(BaseTask):
    application = luigi.Parameter(
        default='whitehall-admin',
        always_in_help=True,
        description=("Application to extract error rates for")
    )

    def __init__(self, *args, **kwargs):
        super().__init__(step_name='error_rate_extract', *args, **kwargs)

        self.segment = self.application

    def output(self):
        return self.snapshot_target()

    def run(self):
        step = ProductionErrorRatesSource()
        data = step.get_error_rates_data(self.application, self.closed_date_range)

        # TODO: because the snapshot is the output, reruns of the job can use
        # invalid data. We should either use something else as the output or
        # use a different filename until the data has been validated.
        self.save_snapshot(data)

        step.validate_output(
            data,
            df_name='output from error_rate_extract {}'.format(self.application)
        )

if __name__ == '__main__':
    luigi.run(main_task_cls=ErrorRatePipelineExtract, local_scheduler=True)
