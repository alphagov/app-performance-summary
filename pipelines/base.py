import luigi
import pandas as pd
import datetime
import logging
import os
from pipeline_util.time_period import previous_month, DateRange
from pipeline_util.pipeline_resource_manager import PipelineResourceManager


class BaseTask(luigi.Task):
    """
    Base class for processing time dependant data.
    """

    date_interval = luigi.DateIntervalParameter(
        default=None,
        always_in_help=True,
        description=("Date interval of report. Defaults to last month.")
    )

    def __init__(self, step_name, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.step_name = step_name
        self.segment = None

        if self.date_interval is None:
            self.closed_date_range = previous_month()
        else:
            one_day = datetime.timedelta(days=1)
            self.closed_date_range = DateRange(start_date=self.date_interval.date_a, end_date=self.date_interval.date_b - one_day)

        self.resource_manager = PipelineResourceManager(date_range=self.closed_date_range)

    def snapshot_target(self):
        return luigi.LocalTarget(
            self.resource_manager.local_output_file_path(
                step_name=self.step_name,
                segment=self.segment
            )
        )

    def save_snapshot(self, data):
        self.resource_manager.save_snapshot(data, self.step_name, self.segment)

    def load_from_step(self, step_name, segment=None):
        return self.resource_manager.load_snapshot(step_name, segment=segment)
