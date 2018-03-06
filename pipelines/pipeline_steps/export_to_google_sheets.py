"""
Load a dataset and its glossary into a google spreadsheet
"""
import logging

class ExportToGoogleSheets:
    def __init__(self, google_drive_client):
        self.client = google_drive_client
        self.logger = logging.getLogger(__name__)

    def write_data(self, data, file_name, glossary_definitions, share_email):
        self.logger.info('Creating spreadsheet: %s', file_name)
        wb = self.client.create_spreadsheet(file_name)

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

        self.logger.debug('Sharing with group %s', share_email)
        wb.share(share_email, role='writer')
