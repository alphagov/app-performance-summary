import datetime
import csv
import os
import pathlib
import requests
from contextlib import contextmanager

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
    response = requests.get(RENDER_URL, params=params, stream=True)
    return csv.reader(response.iter_lines(decode_unicode=True), )


@contextmanager
def output_csv(app):
    '''
    Create a CSV writer for an app
    '''
    outputdir = pathlib.Path('output')
    try:
        outputdir.mkdir()
    except FileExistsError:
        pass

    filename = app.name + '.csv'
    full_path = outputdir / filename
    with full_path.open('w', newline='') as csvfile:
        yield csv.writer(csvfile)


def combine_datasets(error_response, all_response):
    '''
    Combine the two CSVs into one data set
    '''
    error_row = next(error_response)

    for total_row in all_response:
        error_timestamp = error_row[1] if error_row else None
        total_timestamp = total_row[1]

        if total_row[2] == '':
            print('warning: no data for {timestamp}'.format(timestamp=total_timestamp))
            continue

        # Every sample in the totals data should be in the errors data
        assert (error_row is None) or (error_timestamp <= total_timestamp)

        if error_timestamp == total_timestamp:
            yield [total_timestamp, error_row[2], total_row[2]]

            # Only iterate the error response if the data matches
            try:
                error_row = next(error_response)
            except StopIteration:
                error_row = None
        else:
            yield [total_timestamp, None, total_row[2]]


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

        with output_csv(app) as writer:
            writer.writerow(OUTPUT_HEADERS)

            for combined_row in combine_datasets(error_response, all_response):
                writer.writerow(combined_row)
