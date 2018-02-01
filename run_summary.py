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
METRIC_PATTERN = 'sum(stats_counts.*.nginx_logs.{app_name}_publishing_service_gov_uk.http_{status})'
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
    response = requests.get(RENDER_URL, params=params, stream=True, timeout=10)
    return csv.reader(response.iter_lines(decode_unicode=True), )


@contextmanager
def output_csv(app, report_period):
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

        with output_csv(app, report_month) as writer:
            writer.writerow(OUTPUT_HEADERS)

            for combined_row in combine_datasets(error_response, all_response):
                writer.writerow(combined_row)
