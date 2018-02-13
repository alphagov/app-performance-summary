'''
Extract step for application error rates pipeline
'''
import pathlib
from pipeline_steps.production_error_rates_source import ProductionErrorRatesSource
from pipeline_util.file_name_utility import FileNameUtility
from pipeline_util.time_period import previous_month

APP_NAMES = [
    'asset-manager',
    'bouncer',
    'collections-publisher',
    'contacts-admin',
    'content-performance-manager',
    'content-store',
    'content-tagger',
    'email-alert-api',
    'hmrc-manuals-api',
    'imminence',
    'local-links-manager',
    'manuals-publisher',
    'mapit',
    'maslow',
    'policy-publisher',
    'publisher',
    'publishing-api',
    'release',
    'rummager',
    'search-admin',
    'short-url-manager',
    'signon',
    'specialist-publisher',
    'support',
    'support-api',
    'transition',
    'travel-advice-publisher',
    'whitehall-admin',

    # Frontend
    'calculators',
    'calendars',
    'government-frontend',
    'info-frontend',
    'manuals-frontend',
    'smartanswers',
    'whitehall-frontend',
    'feedback',
    'finder-frontend',
    'collections',
    'static'
]


def output_csv(app_name, report_period, data):
    '''
    Create a CSV writer for an app
    '''
    outputdir = pathlib.Path('..') / 'output'
    try:
        outputdir.mkdir()
    except FileExistsError:
        pass

    filename_utility = FileNameUtility()
    filename = filename_utility.get_time_range_file_name(
        base_file_name='error_rates',
        segment=app_name,
        start_date = report_period.start_date,
        end_date = report_period.end_date
    )

    full_path = outputdir / filename
    data.to_csv(full_path, index=False)


def run():
    report_month = previous_month()

    for app_name in APP_NAMES:
        print('Fetching {app_name} {report_month}'.format(app_name=app_name, report_month=report_month))

        step = ProductionErrorRatesSource()
        data = step.get_error_rates_data(app_name, report_month)
        output_csv(app_name, report_month, data)


if __name__ == '__main__':
    run()
