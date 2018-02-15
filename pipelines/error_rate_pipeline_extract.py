'''
Extract step for application error rates pipeline
'''
import luigi
import datetime
import pathlib
from os.path import dirname
from pipeline_steps.production_error_rates_source import ProductionErrorRatesSource
from pipeline_util.file_name_utility import FileNameUtility
from pipeline_util.time_period import previous_month, DateRange


class ErrorRatePipelineExtract(luigi.Task):
    date_interval = luigi.DateIntervalParameter(
        default=None,
        always_in_help=True,
        description=("Date interval of report. Defaults to last month.")
    )

    application = luigi.Parameter(
        default='whitehall-admin',
        always_in_help=True,
        description=("Application to extract error rates for"))

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.file_name_utility = FileNameUtility()

        if self.date_interval is None:
            self.closed_date_range = previous_month()
        else:
            one_day = datetime.timedelta(days=1)
            self.closed_date_range = DateRange(start_date=self.date_interval.date_a, end_date=self.date_interval.date_b - one_day)

    def output(self):
        return luigi.LocalTarget(self._output_file_name())

    def run(self):
        print('Fetching {app_name} {date_range}'.format(app_name=self.application, date_range=self.closed_date_range))
        step = ProductionErrorRatesSource()
        data = step.get_error_rates_data(self.application, self.closed_date_range)
        data.to_csv(self._output_file_name(), index=False)

    def _output_file_name(self):
        filename = self.file_name_utility.get_time_range_file_name(
            base_file_name='error_rates',
            segment=self.application,
            start_date = self.closed_date_range.start_date,
            end_date = self.closed_date_range.end_date
        )

        full_path = self._output_dir() / filename
        return str(full_path)

    def _output_dir(self):
        outputdir = pathlib.Path(__file__).parent / '..' / 'output'
        try:
            outputdir.mkdir()
        except FileExistsError:
            pass
        return outputdir


if __name__ == '__main__':
    luigi.run(main_task_cls=ErrorRatePipelineExtract, local_scheduler=True)
