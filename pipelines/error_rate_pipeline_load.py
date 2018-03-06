'''
Extract step for application error rates pipeline
'''
import luigi
from pipeline_steps.export_to_google_sheets import ExportToGoogleSheets
from pipeline_steps.production_error_rates_source import ProductionErrorRatesSource
from error_rate_pipeline_extract import ErrorRatePipelineExtract
from pipeline_util.google_sheet_client import GoogleSheetClient, GoogleSheetTarget
from base import BaseTask
import pandas as pd
import os

class ErrorRatePipelineLoad(BaseTask):
    def requires(self):
        return [
            ErrorRatePipelineExtract(application='whitehall-admin', date_interval=self.date_interval),
            ErrorRatePipelineExtract(application='whitehall-frontend', date_interval=self.date_interval),
        ]

    def __init__(self, *args, **kwargs):
        super().__init__(step_name='error_rate_extract', *args, **kwargs)

        self.filename = filename = self.resource_manager.output_file_name(
            step_name=self.step_name,
            segment=self.segment
        ).replace('.csv', '')

        self.glossary = ProductionErrorRatesSource.glossary()
        self.gsheet_share_email = os.environ['PLATFORM_METRICS_MAILING_LIST']

    def output(self):
        return GoogleSheetTarget(self.filename)

    def run(self):
        for application in ('whitehall-admin', 'whitehall-frontend'):
            df = self.load_from_step('error_rate_extract', application)

            export_step = ExportToGoogleSheets(GoogleSheetClient())

            export_step.validate_input(
                df,
                df_name='input from error_rate_extract {}'.format(application)
            )

            export_step.write_data(
                df,
                self.filename,
                self.glossary,
                share_email=self.gsheet_share_email
            )

if __name__ == '__main__':
    luigi.run(main_task_cls=ErrorRatePipelineLoad, local_scheduler=True)
