'''
Extract step for application error rates pipeline
'''
import luigi
from pipeline_steps.export_to_google_sheets import ExportToGoogleSheets
from error_rate_pipeline_extract import ErrorRatePipelineExtract
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

        # FIXME
        self.glossary = glossary = pd.DataFrame([['foo', "dfsfsdfd"], ["bar", "dssfsfd"], ["baz", "fdssffs"]], columns=["name", "description"])

    def output(self):
        return self.snapshot_target() # FIXME

    def run(self):
        gsheet_share_email = os.environ['PLATFORM_METRICS_MAILING_LIST']
        for application in ('whitehall-admin', 'whitehall-frontend'):
            df = self.load_from_step('error_rate_extract', application)
            filename = self.resource_manager.output_file_name(
                step_name=self.step_name,
                segment=self.segment
            ).replace('.csv', '')
            ExportToGoogleSheets().write_data(df, filename, self.glossary, share_email=gsheet_share_email)

if __name__ == '__main__':
    luigi.run(main_task_cls=ErrorRatePipelineLoad, local_scheduler=True)
