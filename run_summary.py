import datetime
import os
import pathlib
import pandas as pd
from urllib.parse import urlencode

# This query combines all different machines and different status codes.
# This could obscure cases where a particular machine has a much worse error
# rate than the others, but we want to make sure that all user requests are
# counted.
METRIC_PATTERN = 'sum(stats_counts.*.nginx_logs.{app_name}*.http_{status})'
GRAPHITE_URL = os.environ.get('GRAPHITE_URL', 'https://graphite.publishing.service.gov.uk')
RENDER_URL = GRAPHITE_URL + '/render/?format=csv'
OUTPUT_HEADERS = ['Timestamp', '5xx Responses', 'Total Responses']
GRAPHITE_DATE = '%Y%m%d'


class App:
    '''
    An application in graphite
    '''
    def __init__(self, name):
        self.name = name

    def __str__(self):
        return self.name

    def response_count_query(self, status='*'):
        return METRIC_PATTERN.format(
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
    App(name='asset-manager'),
    App(name='bouncer'),
    App(name='collections-publisher'),
    App(name='contacts-admin'),
    App(name='content-performance-manager'),
    App(name='content-store'),
    App(name='content-tagger'),
    App(name='email-alert-api'),
    App(name='hmrc-manuals-api'),
    App(name='imminence'),
    App(name='local-links-manager'),
    App(name='manuals-publisher'),
    App(name='mapit'),
    App(name='maslow'),
    App(name='policy-publisher'),
    App(name='publisher'),
    App(name='publishing-api'),
    App(name='release'),
    App(name='rummager'),
    App(name='search-admin'),
    App(name='short-url-manager'),
    App(name='signon'),
    App(name='specialist-publisher'),
    App(name='support'),
    App(name='support-api'),
    App(name='transition'),
    App(name='travel-advice-publisher'),
    App(name='whitehall-admin'),

    # Frontend
    App(name='calculators'),
    App(name='calendars'),
    App(name='government-frontend'),
    App(name='info-frontend'),
    App(name='manuals-frontend'),
    App(name='smartanswers'),
    App(name='whitehall-frontend'),
    App(name='feedback'),
    App(name='finder-frontend'),
    App(name='collections'),
    App(name='static')
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
