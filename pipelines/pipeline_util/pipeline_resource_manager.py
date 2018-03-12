import pathlib
import luigi
import pandas as pd
from pipeline_util.file_name_utility import FileNameUtility

class PipelineResourceManager:
    """
    Sets up a consistent naming convention and local file structure for
    pipeline outputs.

    Local files are saved in this structure:

    output
        -> my_pipeline_daily_2015-10-15
            -> snapshots
                ...
            -> logs
                ...
    """
    def __init__(self, date_range):
        self.file_name_utility = FileNameUtility()
        self.date_range = date_range

    def _output_dir(self):
        outputdir = pathlib.Path(__file__).parent / '..' / '..' / 'output'
        try:
            outputdir.mkdir()
        except FileExistsError:
            pass
        return outputdir

    def local_output_file_path(self, step_name, segment=None):
        filename = self.output_file_name(step_name, segment)
        full_path = self._output_dir() / filename
        return str(full_path)

    def output_file_name(self, step_name, segment=None):
        return self.file_name_utility.get_time_range_file_name(
            base_file_name=step_name,
            segment=segment,
            start_date = self.date_range.start_date,
            end_date = self.date_range.end_date
        )

    def load_snapshot(self, step_name, segment=None):
        file_path = self.local_output_file_path(step_name=step_name, segment=segment)
        df = pd.read_csv(
            file_path,
            index_col=False
        )
        return df

    def save_snapshot(self, data, step_name, segment=None):
        data.to_csv(
            self.local_output_file_path(step_name=step_name, segment=segment),
            index=False
        )
