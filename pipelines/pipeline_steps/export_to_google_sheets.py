"""
Load a dataset and its glossary into a google spreadsheet
"""

class ExportToGoogleSheets:
    def __init__(self, google_drive_client):
        self.client = google_drive_client

    def write_data(self, data, file_name, glossary_definitions, share_email):
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

        wb.share(share_email, role='writer')
