"""
Extract error rate metrics from graphite.
"""
from pipeline_util.pipeline_configuration import PipelineConfiguration
from pipeline_util.graphite_extract_utility import GraphiteExtractUtility
from pipeline_util.glossary import GlossaryBuilder
import pandas as pd
import os

METRIC_PATTERN = 'sum(stats_counts.*.nginx_logs.{app_name}*.http_{status})'
GRAPHITE_DATE = '%Y%m%d'


class GraphiteQueryGenerator:
    '''
    Generates a graphite query for application HTTP counts
    '''
    def __init__(self, application, time_period):
        self.application = application
        self.start_date = time_period.start_date
        self.end_date = time_period.end_date

    def target(self, status='*'):
        return METRIC_PATTERN.format(
            app_name=self.application,
            status=status
        )

    def params(self, status='*'):
        return {
            'from': self.start_date.strftime(GRAPHITE_DATE),
            'until': self.end_date.strftime(GRAPHITE_DATE),
            'target': self.target(status=status)
        }


class ProductionErrorRatesSource:
    """
    Provides error rates data extracted from graphite.  The data source is used to help
    demonstrate the behaviour of the pipeline using fixed data.
    """
    @staticmethod
    def glossary():
        g = GlossaryBuilder()
        g.describe_variable('timestamp', 'Timestamp defining the bucket of time error rates were counted over.')
        g.describe_variable('count_total', 'Total number of responses served during the time period')
        g.describe_variable('count_error', 'Total number of error responses (an HTTP status code of 500-599) served during the time period')
        return g.glossary

    def __init__(self, default_url='https://graphite.publishing.service.gov.uk'):
        self.graphite = GraphiteExtractUtility(default_url=default_url)

    def get_error_rates_data(self, app_name, report_month):
        query_generator = GraphiteQueryGenerator(app_name, report_month)

        # All 5xx responses
        params = query_generator.params(status='5*')
        error_response = self.graphite.get_csv(params)

        # All responses
        params = query_generator.params()
        all_response = self.graphite.get_csv(params)

        return pd.merge(
            error_response,
            all_response,
            on='timestamp',
            validate = 'one_to_one',
            how='right',
            suffixes=('_total', '_error')
        )
