import numpy as np
import pandas as pd 
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Path to your service account credentials file
credentials_file = '/prl/.secrets/google-auth.json'
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
        # print(f"Found spreadsheet '{sheetname}' with ID: {spreadsheet_id}")

        # Step 2: Retrieve data from the first sheet
        sheet_metadata = sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        sheet_name = sheet_metadata['sheets'][0]['properties']['title']  # Assuming first sheet
        range_name = f"{sheet_name}!A:ZZ"  # Adjust range as needed
        
        # Fetch the data in the specified range
        result = sheets_service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=range_name
        ).execute()
        
        values = result.get('values', [])
        
        if not values:
            print(f"No data found in sheet: {sheetname}")
            return None

        # Convert the data to a DataFrame, using the first row as headers
        df = pd.DataFrame(values[1:], columns=values[0])  # Skip header row for data
        return df

    except HttpError as e:
        print("An error occurred:", e)
        return None

def push_data(sheetname, folder, data):
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
        print(f"Updating spreadsheet '{sheetname}' with ID: {spreadsheet_id}")

        # Dynamically ensure range fits within grid
        sheet_metadata = sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        sheet_properties = sheet_metadata['sheets'][0]['properties']
        grid_rows = sheet_properties['gridProperties']['rowCount']
        # Fetch the header row to determine the column index dynamically
        header_row = sheets_service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range="A1:ZZ1"  # Fetch the first row (headers)
        ).execute().get('values', [[]])[0]  # Default to an empty list if no headers

        for col in data.columns:
            # print(f'\t===UPDATING {col}!===')
            # Find the index of the column in the header row
            try:
                col_index = header_row.index(col)  # 0-based index of the column
            except ValueError:
                print(f"Column '{col}' not found in the headers. Skipping...")
                continue

            # Convert column index to column letter
            col_letter = ''
            while col_index >= 0:  # Handle multi-letter columns (e.g., 'AA', 'AB')
                col_letter = chr(65 + (col_index % 26)) + col_letter
                col_index = (col_index // 26) - 1

            # Ensure the range is within the grid limits
            # col_data = data[col].to_list()
            col_data = data[col].apply(lambda x: x.item() if isinstance(x, np.generic) else x).tolist()  # Convert NumPy types
            col_data = [c if not (isinstance(c, float) and np.isnan(c)) else None for c in col_data]
            range_end_row = len(col_data) + 1  # Adjust to include all rows
            range_name = f"{col_letter}1:{col_letter}{range_end_row}"

            body = {
                "range": range_name,
                "values": [[col]] + [[val] for val in col_data],  # Include header row
            }

            # Update the column in the spreadsheet
            sheets_service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=range_name,
                valueInputOption="RAW",
                body=body
            ).execute()
            # print(f"Updated column '{col}' in spreadsheet '{sheetname}'.")

        print(f"Spreadsheet '{sheetname}' successfully updated.")

    except HttpError as e:
        print("An error occurred:", e)
        return None
