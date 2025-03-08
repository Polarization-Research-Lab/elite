import os
import time

import numpy as np
import pandas as pd 
import dotenv

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


# Path to your service account credentials file
dotenv.load_dotenv('../../../env'); dotenv.load_dotenv(os.environ['PATH_TO_SECRETS'])

credentials_file = os.environ.get('PATH_TO_GOOGLE_CREDS')

# print(credentials_file)
# exit()
creds = Credentials.from_service_account_file(
    credentials_file, 
    scopes=[
        'https://www.googleapis.com/auth/drive',
        'https://www.googleapis.com/auth/spreadsheets',
    ]
)

# Connect to the Google Drive and Sheets APIs
drive_service = build('drive', 'v3', credentials=creds)
sheets_service = build('sheets', 'v4', credentials=creds)

# # # # # # # # #
# PULL SPREADSHEET AS DATAFRAME
# # # # # # # # #
def pull_data(sheetname, folder):
    try:
        # Step 1: Retrieve the spreadsheet ID based on the sheet name within the specified folder
        query = f"'{folder}' in parents and name='{sheetname}' and mimeType='application/vnd.google-apps.spreadsheet'"
        results = drive_service.files().list(
            q=query,
            fields="files(id, name)",
            includeItemsFromAllDrives=True,
            supportsAllDrives=True
        ).execute()

        files = results.get('files', [])
        
        if not files:
            print(f"No spreadsheet found with the name: {sheetname} in folder: {folder}")
            return None
        
        spreadsheet_id = files[0]['id']
        
        # Step 2: Fetch the data from the sheet
        result = sheets_service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range="A1:ZZ1000",  # Arbitrarily large range to fetch all available rows and columns
            valueRenderOption="UNFORMATTED_VALUE",  # Get raw values
            dateTimeRenderOption="FORMATTED_STRING"  # Date-time as serial numbers
        ).execute()

        values = result.get('values', [])
        
        if not values:
            print(f"No data found in sheet: {sheetname}")
            return None  # Return an empty DataFrame if no data
        
        # Step 3: Ensure all rows have the same length as the header
        headers = values[0]
        data = values[1:]

        # Normalize row lengths by padding or trimming to match the header length
        normalized_data = [row + [''] * (len(headers) - len(row)) if len(row) < len(headers) else row[:len(headers)] for row in data]

        # Step 4: Convert to DataFrame
        df = pd.DataFrame(normalized_data, columns=headers)
        return df

    except Exception as e:
        print(f"Error pulling data. EXITING SCRIPT!!!! {e}")
        raise(e)

# # # # # # # # #
# PUSH DATAFRAME TO SPREADSHEET
# # # # # # # # #
def push_data(sheetname, folder, data):
    try:

        data = data.replace({pd.NaT: None, np.nan: None})
        num_rows, num_cols = data.shape


        # Step 1: Retrieve the spreadsheet ID
        query = f"'{folder}' in parents and name='{sheetname}' and mimeType='application/vnd.google-apps.spreadsheet'"
        results = drive_service.files().list(
            q=query,
            fields="files(id, name)",
            includeItemsFromAllDrives=True,
            supportsAllDrives=True
        ).execute()
        time.sleep(1)

        files = results.get('files', [])
        if not files:
            print(f"No spreadsheet found with the name: {sheetname} in folder: {folder}")
            return None

        spreadsheet_id = files[0]['id']
        print(f"Updating spreadsheet '{sheetname}' with ID: {spreadsheet_id}")


        # Step 2: Paste new data first (including headers)
        range_name = "A1"
        body = {"range": range_name, "values": [data.columns.tolist()] + data.values.tolist()}
        sheets_service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=range_name,
            valueInputOption="RAW",
            body=body
        ).execute()
        time.sleep(1)


        # Step 3: Clear extra columns and rows
        sheet_metadata = sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        sheet_id = sheet_metadata['sheets'][0]['properties']['sheetId']
        grid_props = sheet_metadata['sheets'][0]['properties']['gridProperties']
        sheet_row_count = grid_props['rowCount']
        sheet_col_count = grid_props['columnCount']

        requests = []

        if sheet_col_count > num_cols:
            requests.append({
                "updateCells": {
                    "range": {
                        "sheetId": sheet_id,
                        "startColumnIndex": num_cols,  # Start from the first extra column
                        "endColumnIndex": sheet_col_count  # Remove all excess columns
                    },
                    "fields": "userEnteredValue"  # Clears only text, keeps formatting
                }
            })

        if sheet_row_count > num_rows + 1:  # +1 to account for the header row
            requests.append({
                "updateCells": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": num_rows + 1,  # Start from the first extra row
                        "endRowIndex": sheet_row_count  # Remove all excess rows
                    },
                    "fields": "userEnteredValue"  # Clears only text, keeps formatting
                }
            })

        # Execute batch update if there are changes to apply
        if requests:
            sheets_service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={"requests": requests}
            ).execute()
            time.sleep(1)


        # Step 4: Apply Conditional Formatting Rule (Review Column = B)
        conditional_formatting_request_0 = {
            "addConditionalFormatRule": {
                "rule": {
                    "ranges": [{
                        "sheetId": sheet_id,
                        "startRowIndex": 1,  # Skip header
                        "startColumnIndex": 0,  # Start from column A
                        "endColumnIndex": num_cols  # Apply to all columns
                    }],
                    "booleanRule": {
                        "condition": {
                            "type": "CUSTOM_FORMULA",
                            "values": [{"userEnteredValue": "=$B2=0"}]  # If B2 == 1, apply styling
                        },
                        "format": {
                            "backgroundColor": {"red": 1.0, "green": 1.0, "blue": 1.0}  # Grey
                        }
                    }
                },
                "index": 0
            }
        }
        conditional_formatting_request_1 = {
            "addConditionalFormatRule": {
                "rule": {
                    "ranges": [{
                        "sheetId": sheet_id,
                        "startRowIndex": 1,  # Skip header
                        "startColumnIndex": 0,  # Start from column A
                        "endColumnIndex": num_cols  # Apply to all columns
                    }],
                    "booleanRule": {
                        "condition": {
                            "type": "CUSTOM_FORMULA",
                            "values": [{"userEnteredValue": "=$B2=1"}]  # If B2 == 1, apply styling
                        },
                        "format": {
                            "backgroundColor": {"red": 0.6, "green": 0.6, "blue": 0.6}  # Grey
                        }
                    }
                },
                "index": 0
            }
        }


        sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": [conditional_formatting_request_0, conditional_formatting_request_1]}
        ).execute()


        # Step 5: Hide the last column (assumed to be "id")
        hide_last_column_request = {
            "updateDimensionProperties": {
                "range": {
                    "sheetId": sheet_id,
                    "dimension": "COLUMNS",
                    "startIndex": num_cols - 1,  # Last column index
                    "endIndex": num_cols  # Ensure only the last column is hidden
                },
                "properties": {"hiddenByUser": True},
                "fields": "hiddenByUser"
            }
        }

        sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": [hide_last_column_request]}
        ).execute()
        time.sleep(1)

        print('PAGE UPDATED SUCCESSFULLY')
    except Exception as e:
        print(f'FAILED PUSH WITH ERR:\n---\n{e}\n---\n')
        raise(e)


# # # # # # # # #
# RUN VERSION CHECK
# # # # # # # # #
def get_version_history(sheetname, folder):
    """Retrieve version history for a Google Sheet"""
    
    # Step 1: Retrieve the spreadsheet ID
    query = f"'{folder}' in parents and name='{sheetname}' and mimeType='application/vnd.google-apps.spreadsheet'"
    results = drive_service.files().list(
        q=query,
        fields="files(id, name)",
        includeItemsFromAllDrives=True,
        supportsAllDrives=True
    ).execute()
    time.sleep(1)

    files = results.get('files', [])
    if not files:
        print(f"No spreadsheet found with the name: {sheetname} in folder: {folder}")
        return None

    spreadsheet_id = files[0]['id']
    # print(f"Fetching version history for '{sheetname}' (ID: {spreadsheet_id})")

    # Step 2: Get version history from Drive API
    revisions = drive_service.revisions().list(
        fileId=spreadsheet_id,
        fields="revisions(id, modifiedTime, lastModifyingUser)"
    ).execute()

    # Step 3: Extract relevant info
    version_history = []
    for rev in revisions.get("revisions", []):
        version_history.append({
            "revision_id": rev["id"],
            "modified_time": rev["modifiedTime"],
            "editor": rev.get("lastModifyingUser", {}).get("displayName", "Unknown"),
            "editor_email": rev.get("lastModifyingUser", {}).get("emailAddress", "Unknown"),
            "mime_type": rev.get("mimeType", "Unknown"),  # The type of revision (Google Sheets, Docs, etc.)
            "keep_forever": rev.get("keepForever", False),  # Whether the revision is permanently stored
            "published": rev.get("published", False),  # Whether the revision was explicitly published
            "export_links": rev.get("exportLinks", {}),  # Exportable links (for downloadable formats)
        })
    return version_history