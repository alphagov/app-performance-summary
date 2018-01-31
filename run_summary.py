import datetime
import os
import pathlib
import pandas as pd
from urllib.parse import urlencode

# This query combines all different machines and different status codes.
# This could obscure cases where a particular machine has a much worse error
# rate than the others, but we want to make sure that all user requests are
# counted.
METRIC_PATTERN = 'sum(stats_counts.{host_name}-*_{host_group}.nginx_logs.{app_name}_publishing_service_gov_uk.http_{status})'
GRAPHITE_URL = os.environ.get('GRAPHITE_URL', 'https://graphite.publishing.service.gov.uk')
RENDER_URL = GRAPHITE_URL + '/render/?format=csv'
OUTPUT_HEADERS = ['Timestamp', '5xx Responses', 'Total Responses']
GRAPHITE_DATE = '%Y%m%d'


class App:
    '''
    An application in graphite
    '''
    def __init__(self, name, host_group, host_name):
        self.name = name
        self.host_group = host_group
        self.host_name = host_name

    def __str__(self):
        return self.name

    def response_count_query(self, status='*'):
        return METRIC_PATTERN.format(
            host_name=self.host_name,
            host_group=self.host_group,
            app_name=self.name,
            status=status
        )


class ReportingPeriod:
    '''
    A graphite time period
    '''
    def __init__(self):
        today = datetime.date.today()
        date_to = today.replace(day=1) - datetime.timedelta(days=1)
        date_from = date_to.replace(day=1)

        self.date_from = date_from
        self.date_to = date_to

    def params(self):
        return {
            'from': self.date_from.strftime(GRAPHITE_DATE),
            'until': self.date_to.strftime(GRAPHITE_DATE)
        }

    def __str__(self):
        return self.date_from.strftime(GRAPHITE_DATE) + '_' + self.date_to.strftime(GRAPHITE_DATE)


APPS = [
    App(name='asset-manager', host_name='backend', host_group='backend'),
    App(name='bouncer', host_name='bouncer', host_group='redirector'),
    App(name='collections-publisher', host_name='backend', host_group='backend'),
    App(name='contacts-admin', host_name='backend', host_group='backend'),
    App(name='content-performance-manager', host_name='backend', host_group='backend'),
    App(name='content-store', host_name='content-store', host_group='api'),
    App(name='content-tagger', host_name='backend', host_group='backend'),
    App(name='email-alert-api', host_name='backend', host_group='backend'),
    App(name='hmrc-manuals-api', host_name='backend', host_group='backend'),
    App(name='imminence', host_name='backend', host_group='backend'),
    App(name='local-links-manager', host_name='backend', host_group='backend'),
    App(name='manuals-publisher', host_name='backend', host_group='backend'),
    App(name='mapit', host_name='mapit', host_group='api'),
    App(name='maslow', host_name='backend', host_group='backend'),
    App(name='policy-publisher', host_name='backend', host_group='backend'),
    App(name='publisher', host_name='backend', host_group='backend'),
    App(name='publishing-api', host_name='publishing-api', host_group='backend'),
    App(name='release', host_name='backend', host_group='backend'),
    App(name='rummager', host_name='search', host_group='api'),
    App(name='search-admin', host_name='backend', host_group='backend'),
    App(name='short-url-manager', host_name='backend', host_group='backend'),
    App(name='signon', host_name='backend', host_group='backend'),
    App(name='specialist-publisher', host_name='backend', host_group='backend'),
    App(name='support', host_name='backend', host_group='backend'),
    App(name='support-api', host_name='backend', host_group='backend'),
    App(name='transition', host_name='backend', host_group='backend'),
    App(name='travel-advice-publisher', host_name='backend', host_group='backend'),
    App(name='whitehall-admin', host_name='whitehall-backend', host_group='backend'),

    # Frontend
    App(name='calculators', host_name='calculators-frontend', host_group='frontend'),
    App(name='calendars', host_name='calculators-frontend', host_group='frontend'),
    App(name='government-frontend', host_name='frontend', host_group='frontend'),
    App(name='info-frontend', host_name='frontend', host_group='frontend'),
    App(name='manuals-frontend', host_name='frontend', host_group='frontend'),
    App(name='smartanswers', host_name='calculators-frontend', host_group='frontend'),
    App(name='whitehall-frontend', host_name='whitehall-frontend', host_group='frontend'),
    App(name='feedback', host_name='frontend', host_group='frontend'),
    App(name='finder-frontend', host_name='calculators-frontend', host_group='frontend'),
    App(name='collections', host_name='frontend', host_group='frontend'),
    App(name='static', host_name='frontend', host_group='frontend')
]


def get_csv(params):
    '''
    Fetch a CSV from the graphite API
    '''
    query_params = urlencode(params)
    full_url = RENDER_URL + '&' + query_params

    return pd.read_csv(full_url, header=None, names=('timestamp', 'count'), usecols=(1,2))


def output_csv(app, report_period, data):
    '''
    Create a CSV writer for an app
    '''
    outputdir = pathlib.Path('output')
    try:
        outputdir.mkdir()
    except FileExistsError:
        pass

    filename = '{app_name}_{report_period}.csv'.format(
        app_name=app.name,
        report_period=report_period
    )

    full_path = outputdir / filename
    data.to_csv(full_path, index=False)


def combine_datasets(error_response, all_response):
    '''
    Combine the two CSVs into one data set
    '''
    return pd.merge(
        error_response,
        all_response,
        on='timestamp',
        validate = 'one_to_one',
        how='right',
        suffixes=('_total', '_error')
    )


if __name__ == '__main__':
    report_month = ReportingPeriod()

    for app in APPS:
        print('Fetching {app} {report_month}'.format(app=app, report_month=report_month))

        # All 5xx responses
        params = dict(target=app.response_count_query('5*'))
        params.update(report_month.params())
        error_response = get_csv(params)

        # All responses
        params = dict(target=app.response_count_query())
        params.update(report_month.params())
        all_response = get_csv(params)

        data = combine_datasets(error_response, all_response)

        output_csv(app, report_month, data)
