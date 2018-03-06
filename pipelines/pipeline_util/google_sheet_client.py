"""
Classes for writing to google spreadsheets.
"""
import os
import pygsheets
from oauth2client.service_account import ServiceAccountCredentials
from pygsheets.client import SCOPES
from pygsheets import SpreadsheetNotFound


class GoogleSheetTarget:
    """
    Luigi target for an output in google drive
    """
    def __init__(self, filename):
        self.client = GoogleSheetClient()
        self.filename = filename

    def exists(self):
        return self.client.exists(self.filename)


class GoogleSheetClient:
    """
    Wrapper for pygsheets to simplify credential management.
    """
    def __init__(self):
        self.gc = pygsheets.Client(oauth=self.credentials())

    def exists(self, file_name):
        try:
            self.gc.open(file_name)
        except SpreadsheetNotFound:
            return False
        else:
            return True

    def create_spreadsheet(self, file_name):
        """
        Create a new spreadsheet.
        Returns an instance of `pygsheets.Spreadsheet`
        """
        return self.gc.create(file_name)

    def credentials(self):
        '''
        Instantiate credentials manually instead of using pygsheets.authorize,
        so that we can pass everything in through the environment instead of
        managing JSON credentials files.
        '''
        parsed_keyfile = {
            'type': 'service_account',
            'client_email': os.environ['GOOGLE_CLIENT_EMAIL'],
            'private_key': os.environ['GOOGLE_PRIVATE_KEY'],
            'private_key_id': os.environ['GOOGLE_PRIVATE_KEY_ID'],
            'client_id': os.environ['GOOGLE_CLIENT_ID']
        }

        return ServiceAccountCredentials._from_parsed_json_keyfile(
            parsed_keyfile,
            scopes=SCOPES
        )
