import os
import pygsheets
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
from pygsheets.client import SCOPES

class ExportToGoogleSheets:
    def write_data(self, data, file_name, glossary_definitions, share_email):
        gc = pygsheets.Client(oauth=self.credentials())

        wb = gc.create(file_name)
        sheet1 = wb.sheet1
        sheet1.title = 'Data'

        sheet1.set_dataframe(
            data,
            start='A1',
            copy_index=False,
            copy_head=True,
            fit=True)

        wb.add_worksheet(title='Data Dictionary').set_dataframe(
            glossary_definitions,
            start='A1',
            copy_index=False,
            copy_head=True,
            fit=True)


        wb.share(share_email, role='writer')

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


if __name__ == '__main__':
    gsheet_share_email = os.environ['GSHEET_SHARE_EMAIL']
    df = pd.DataFrame([[1,2,3], [4,5,6], [7,8,9]], columns=["foo", "bar", "baz"])
    glossary = pd.DataFrame([['foo', "dfsfsdfd"], ["bar", "dssfsfd"], ["baz", "fdssffs"]], columns=["name", "description"])
    ExportToGoogleSheets().write_data(df, "Data pipeline test spreadsheet", glossary, share_email=gsheet_share_email)
