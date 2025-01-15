import time
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
        
        # Step 2: Fetch the data from the sheet
        result = sheets_service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range="A1:ZZ1000"  # Arbitrarily large range to fetch all available rows and columns
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
        print(f"Error pulling data: {e}")
        return None



def push_data(sheetname, folder, data):
    data['serving_position_since'] = data['serving_position_since'].apply(
        lambda x: x.isoformat() if pd.notna(x) else None
    )
    data['last_updated'] = data['last_updated'].apply(
        lambda x: x.isoformat() if pd.notna(x) else None
    )
    try:
        data = data.sort_values(by="reviewed", ascending=True).reset_index(drop=True)

        # Step 1: Retrieve the spreadsheet ID based on the sheet name within the specified folder
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

        # Step 2: Clear all rows except the first row
        # Fetch sheet metadata to get sheet ID
        sheet_metadata = sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        time.sleep(1)

        sheet_id = sheet_metadata['sheets'][0]['properties']['sheetId']

        # Clear all rows except the header (row 1)
        requests = [{
            "updateCells": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 1,  # Clear everything starting from the second row
                    "endRowIndex": sheet_metadata['sheets'][0]['properties']['gridProperties']['rowCount']
                },
                "fields": "userEnteredValue"  # Clear cell content
            }
        }]
        sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": requests}
        ).execute()
        time.sleep(1)



        # Step 3: Apply white background to all rows
        requests = [{
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 1,  # Clear everything starting from the second row
                    "endRowIndex": sheet_metadata['sheets'][0]['properties']['gridProperties']['rowCount']
                },
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": {"red": 1, "green": 1, "blue": 1}  # White background
                    }
                },
                "fields": "userEnteredFormat.backgroundColor"
            }
        }]
        sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": requests}
        ).execute()
        time.sleep(1)


        # Step 4: Dynamically push data column by column based on matching headers
        # Fetch the header row from the spreadsheet
        header_row = sheets_service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range="A1:ZZ1"  # Fetch the first row (headers)
        ).execute().get('values', [[]])[0]  # Default to an empty list if no headers
        time.sleep(1)


        # Create a mapping of column names to spreadsheet column letters
        header_mapping = {}
        for idx, header in enumerate(header_row):
            col_letter = ''
            col_index = idx
            while col_index >= 0:
                col_letter = chr(65 + (col_index % 26)) + col_letter
                col_index = (col_index // 26) - 1
            header_mapping[header] = col_letter

        # print(f"Header mapping: {header_mapping}")  # Debugging log

        # Push data column by column
        for col in data.columns:
            try:
                # Find the corresponding column letter from the header mapping
                if col not in header_mapping:
                    print(f"Column '{col}' not found in spreadsheet headers. Skipping...")
                    continue

                col_letter = header_mapping[col]

                # Prepare the column data (starting from row 2)
                col_data = data[col].apply(lambda x: x.item() if isinstance(x, np.generic) else x).tolist()
                col_data = [c if not (isinstance(c, float) and pd.isna(c)) else None for c in col_data]
                range_start_row = 2  # Row 2 (after the header)
                range_end_row = len(col_data) + range_start_row - 1
                range_name = f"{col_letter}{range_start_row}:{col_letter}{range_end_row}"

                # Log for debugging purposes
                # print(f"Updating column '{col}' in range '{range_name}' with data: {col_data}")

                # Prepare the body
                body = {
                    "range": range_name,
                    "values": [[val] for val in col_data],  # Format values row by row
                }

                # Update the column
                sheets_service.spreadsheets().values().update(
                    spreadsheetId=spreadsheet_id,
                    range=range_name,
                    valueInputOption="RAW",
                    body=body
                ).execute()
                time.sleep(1)

                # print(f"Successfully updated column '{col}'.")

            except Exception as e:
                print(f"Error updating column '{col}': {e}")

        # Step 5: Change background color for rows where "reviewed" == 1
        # Find the index of the "reviewed" column
        reviewed_col_index = data.columns.get_loc("reviewed")
        
        # Get rows where "reviewed" == 1
        reviewed_rows = data.index[data["reviewed"] == '1'].tolist()
        
        # Prepare requests to update the background color
        requests = []
        for row in reviewed_rows:
            requests.append({
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": row + 1,  # Convert DataFrame index to spreadsheet row (1-based, skip header)
                        "endRowIndex": row + 2,
                        "startColumnIndex": 0,
                        "endColumnIndex": len(header_row),  # Full width of the sheet
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "backgroundColor": {"red": 0.6, "green": 0.6, "blue": 0.6}  # Light grey
                        }
                    },
                    "fields": "userEnteredFormat.backgroundColor"
                }
            })

        # Execute the batch update for background colors
        if requests:
            sheets_service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={"requests": requests}
            ).execute()
            time.sleep(1)

            print(f"Updated background color for {len(reviewed_rows)} reviewed rows.")
        else:
            print("No rows found with reviewed = 1.")


        # Step 6: Hide specified columns
        # Columns to hide
        columns_to_hide = ['last_reviewer', 'last_updated', 'id']
        
        # Find the column letters for the columns to hide
        hide_requests = []
        for col in columns_to_hide:
            if col in header_mapping:
                col_letter = header_mapping[col]
                col_index = list(header_mapping.keys()).index(col)  # Get 0-based index from header_mapping
                
                # Create a request to hide the column
                hide_requests.append({
                    "updateDimensionProperties": {
                        "range": {
                            "sheetId": sheet_id,
                            "dimension": "COLUMNS",
                            "startIndex": col_index,  # 0-based index of the column
                            "endIndex": col_index + 1
                        },
                        "properties": {
                            "hiddenByUser": True  # Hide the column
                        },
                        "fields": "hiddenByUser"
                    }
                })

        # Execute the batch update to hide the columns
        if hide_requests:
            sheets_service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={"requests": hide_requests}
            ).execute()
            time.sleep(1)

            print(f"Successfully hid columns: {', '.join(columns_to_hide)}")
        else:
            print("No specified columns to hide.")


        print(f"Spreadsheet '{sheetname}' successfully updated and styled.")


    except HttpError as e:
        print("An error occurred:", e)
        return None
