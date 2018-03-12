'''
Utility for extracting data from the graphite API
'''
import os
from urllib.parse import urlencode
import pandas as pd


class GraphiteExtractUtility:
    def __init__(self, default_url):
        url = os.environ.get('GRAPHITE_URL', default_url)
        self.render_url = url + '/render/?format=csv'

    def get_csv(self, params, metric_name='count'):
        '''
        Fetch a CSV from the graphite API
        '''
        query_params = urlencode(params)
        full_url = self.render_url + '&' + query_params

        return pd.read_csv(full_url, header=None, names=('timestamp', metric_name), usecols=(1,2))
