import os, json, urllib.request, time
from datetime import datetime

import dotenv
import ibis; from ibis import _
import dataset
import numpy as np 
import pandas as pd
import json5
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


import google_utils

# # # # # # # #  
# SETUP
# # # # # # # # 
columns = [
    "name", "reviewed", "first_name", "last_name", "middle_name", 
    "nick_name", "gender", "party", "email",  
    "position", "district", "campaign_website", "government_website", 
    "twitter_handle", "facebook", "instagram", "linkedin", "youtube", 
    "truth_social", "tiktok", "suffix", "last_reviewer", "last_updated", "id"
]

# Path to your service account credentials file
dotenv.load_dotenv('../../../env'); dotenv.load_dotenv(os.environ['PATH_TO_SECRETS'])
credentials_file = os.environ['PATH_TO_GOOGLE_CREDS']

creds = Credentials.from_service_account_file(
    credentials_file, 
    scopes = [
        'https://www.googleapis.com/auth/drive',
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/script.projects',
        'https://www.googleapis.com/auth/script.deployments',
    ]
)

# Connect to the Google Drive and Sheets APIs
drive_service = build('drive', 'v3', credentials=creds)
sheets_service = build('sheets', 'v4', credentials=creds)
script_service = build('script', 'v1', credentials=creds)

filters_for_these = {
    'party': 'dropdown',
    'position': 'dropdown',
    'gender': 'dropdown',
    # 'active': 'checkbox',
    'serving_position_since': 'calendar',
    # 'serving_public_since': 'calendar',
}

shared_folder_id_global = os.environ['PATH_TO_GOOGLE_CREDS']


dotenv.load_dotenv('../../../env'); dotenv.load_dotenv(os.environ['PATH_TO_SECRETS'])

params = f"{os.environ['DB_DIALECT']}://{os.environ['DB_USER']}:{urllib.parse.quote(os.environ['DB_PASSWORD'])}@localhost:{os.environ['DB_PORT']}/elite"

db = ibis.mysql.connect(
    host=os.environ['DB_HOST'],
    user=os.environ['DB_USER'],
    password=os.environ['DB_PASSWORD'],
    database='elite',
)

# Fetch the officials_state table structure and states list
officials_state = (
    db.table('officials')
    .filter([_.level == 'state', _.active == 1])
    .execute()
    .replace({pd.NaT: None})

)
states = (
    db.table('officials')
    .group_by(_.state)
    .aggregate()
    .execute()
    ['state']
    .to_list()
)

# # # # # # # #  
# BUILD FUNCS
# # # # # # # #
def create_sheet_if_not_exists(state_name, shared_folder_id=shared_folder_id_global):
    """
    Checks if a Google Spreadsheet with the given state_name exists in the shared folder.
    If it doesn't exist, creates a new spreadsheet with that name in the specified folder.
    
    Args:
    - state_name (str): The name of the spreadsheet to check or create.
    - shared_folder_id (str): The ID of the shared folder where the sheet should be located.
    """
    try:
        # Step 1: Check if the file already exists in the shared folder
        query = f"'{shared_folder_id}' in parents and name='{state_name}' and mimeType='application/vnd.google-apps.spreadsheet'"
        results = drive_service.files().list(
            q=query,
            fields="files(id, name)",
            includeItemsFromAllDrives=True,
            supportsAllDrives=True
        ).execute()
        files = results.get('files', [])

        if files:
            print(f"Spreadsheet '{state_name}' already exists with ID: {files[0]['id']}")
            return files[0]['id']  # Return the ID of the existing spreadsheet
        
        # Step 2: Create a new spreadsheet if it does not exist
        spreadsheet_body = {
            'properties': {
                'title': state_name
            }
        }
        
        spreadsheet = sheets_service.spreadsheets().create(body=spreadsheet_body).execute()
        spreadsheet_id = spreadsheet.get('spreadsheetId')
        
        # Step 3: Move the new spreadsheet to the shared folder
        # Ensure Shared Drive support in the request
        drive_service.files().update(
            fileId=spreadsheet_id,
            addParents=shared_folder_id,
            fields='id, parents',
            supportsAllDrives=True
        ).execute()
        
        print(f"Spreadsheet '{state_name}' created and moved to shared folder with ID: {spreadsheet_id}")
        return spreadsheet_id

    except HttpError as e:
        print("An error occurred:", e)

def build_sheet(state_name,  stateleg, shared_folder_id=os.environ['PATH_TO_GOOGLE_CREDS']):
    # Create or retrieve the spreadsheet ID
    sheet_id = create_sheet_if_not_exists(state_name, shared_folder_id)    

    try:
        print(f"Editing '{state_name}' | {sheet_id}")
        
        # Clear the header row to ensure it starts fresh
        # sheets_service.spreadsheets().values().clear(
        #     spreadsheetId=sheet_id,
        #     range="A:ZZ"  # Adjust this range as needed to fit your headers
        # ).execute()
        sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=sheet_id,
            body={
                "requests": [
                    {
                        "repeatCell": {
                            "range": {
                                "sheetId": 0,  # Assuming sheet ID 0 for the first/default sheet
                                "startRowIndex": 0,
                                "startColumnIndex": 0,
                                "endColumnIndex": 702,  # Adjust as needed for columns; 702 covers up to "ZZ"
                                "endRowIndex": 1000   # Adjust row count as needed
                            },
                            "cell": {
                                "userEnteredFormat": {},      # Clears formatting
                                "dataValidation": None,       # Clears data validation (dropdowns, checkboxes)
                                "userEnteredValue": None      # Clears values
                            },
                            "fields": "userEnteredFormat,dataValidation,userEnteredValue"
                        }
                    }
                ]
            }
        ).execute()
        print(f"cleared")

        # =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  = 

        # Write headers to the first row, ensuring it updates in place
        sheets_service.spreadsheets().values().update(
            spreadsheetId=sheet_id,
            range="A1:ZZ1",  # Adjust to the specific columns of your headers
            valueInputOption="RAW",
            body={"values": [headers]}
        ).execute()

        # Set black background with white text for the header row
        sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=sheet_id,
            body={
                "requests": [
                    {
                        "repeatCell": {
                            "range": {
                                "sheetId": 0,  # Default sheet ID, typically 0 for the first sheet
                                "startRowIndex": 0,
                                "endRowIndex": 1,
                            },
                            "cell": {
                                "userEnteredFormat": {
                                    "backgroundColor": {"red": 0, "green": 0, "blue": 0},
                                    "textFormat": {"foregroundColor": {"red": 1, "green": 1, "blue": 1}, "bold": True}
                                }
                            },
                            "fields": "userEnteredFormat(backgroundColor, textFormat)"
                        }
                    }
                ]
            }
        ).execute()
        print(f"applied Headers and styling")

        # =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  = 

        # Freeze the first row
        sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=sheet_id,
            body={
                "requests": [
                    {
                        "updateSheetProperties": {
                            "properties": {
                                "sheetId": 0,  # Typically 0 for the first sheet
                                "gridProperties": {
                                    "frozenRowCount": 1,
                                    "frozenColumnCount": 2,  # Freeze the first two columns
                                }
                            },
                            "fields": "gridProperties.frozenRowCount",
                            "fields": "gridProperties.frozenColumnCount",
                        }
                    }
                ]
            }
        ).execute()
        print(f"froze first row")

        # =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  = 

        # 3. Apply conditional formatting and data validation rules for each specified column
        for column in stateleg.columns:
            if column in filters_for_these:
                filter_type = filters_for_these[column]
                
                if filter_type == 'checkbox':
                    # Apply checkbox formatting
                    sheets_service.spreadsheets().batchUpdate(
                        spreadsheetId=sheet_id,
                        body={
                            "requests": [
                                {
                                    "repeatCell": {
                                        "range": {
                                            "sheetId": 0,
                                            "startRowIndex": 1,  # Skip header row
                                            "startColumnIndex": headers.index(column),
                                            "endColumnIndex": headers.index(column) + 1
                                        },
                                        "cell": {
                                            "dataValidation": {
                                                "condition": {
                                                    "type": "BOOLEAN"
                                                },
                                                "strict": True,
                                                "showCustomUi": True
                                            },
                                            "userEnteredFormat": {
                                                "numberFormat": {
                                                    "type": "NUMBER",
                                                    "pattern": "BOOLEAN"  # Specifies checkbox format
                                                }
                                            }
                                        },
                                        "fields": "dataValidation,userEnteredFormat.numberFormat"
                                    }
                                }
                            ]
                        }
                    ).execute()
                    print(f"Applied checkbox for column '{column}'")

                elif filter_type == 'dropdown':
                    # Set dropdown with unique options
                    unique_values = stateleg[column].unique().tolist()
                    sheets_service.spreadsheets().batchUpdate(
                        spreadsheetId=sheet_id,
                        body={
                            "requests": [
                                {
                                    "repeatCell": {
                                        "range": {
                                            "sheetId": 0,
                                            "startRowIndex": 1,
                                            "startColumnIndex": headers.index(column),
                                            "endColumnIndex": headers.index(column) + 1
                                        },
                                        "cell": {
                                            "dataValidation": {
                                                "condition": {
                                                    "type": "ONE_OF_LIST",
                                                    "values": [{"userEnteredValue": str(value)} for value in unique_values]
                                                },
                                                "strict": True,
                                                "showCustomUi": True
                                            }
                                        },
                                        "fields": "dataValidation"
                                    }
                                }
                            ]
                        }
                    ).execute()
                    print(f"Applied dropdown for column '{column}' with options {unique_values}")

                elif filter_type == 'calendar':
                    # Set date format for the column
                    sheets_service.spreadsheets().batchUpdate(
                        spreadsheetId=sheet_id,
                        body={
                            "requests": [
                                {
                                    "repeatCell": {
                                        "range": {
                                            "sheetId": 0,
                                            "startRowIndex": 1,  # Skip header row
                                            "startColumnIndex": headers.index(column),
                                            "endColumnIndex": headers.index(column) + 1
                                        },
                                        "cell": {
                                            "userEnteredFormat": {
                                                "numberFormat": {
                                                    "type": "DATE",
                                                    "pattern": "yyyy-mm-dd"  # Customize the date format if needed
                                                }
                                            }
                                        },
                                        "fields": "userEnteredFormat.numberFormat"
                                    }
                                }
                            ]
                        }
                    ).execute()
                    print(f"Applied date picker format for column '{column}'")

        # =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  = 

        # Filter the data for the given state
        state_data = stateleg[stateleg['state'] == state_name]
        state_data = state_data.replace({pd.NaT: None})
        state_data = state_data.sort_values(by='reviewed', ascending=True)


        state_data['last_updated'] = state_data['last_updated'].astype(str)
        state_data['serving_public_since'] = state_data['serving_public_since'].astype(str)
        state_data['serving_position_since'] = state_data['serving_position_since'].astype(str)
        state_data = state_data.replace({'NaT': ''})

        for col in filters_for_these:
            if filters_for_these[col] == 'checkbox':
                state_data[col] = state_data[col].astype(bool)


        # Convert the DataFrame to a list of lists for easy insertion into the spreadsheet
        values = state_data[headers].values.tolist()

        # Convert all None values in the data to empty strings
        values = [[cell if cell not in [None, 'None', 'NaT'] else "" for cell in row] for row in values]

        # Insert the filtered data into the sheet, starting from the second row (row index 1)
        sheets_service.spreadsheets().values().update(
            spreadsheetId=sheet_id,
            range="A2",  # Start from the second row, first column
            valueInputOption="RAW",
            body={"values": values}
        ).execute()
        print(f"Added data for '{state_name}' into the sheet.")

        # =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  = 

        # Find column indexes by name for conditional formatting
        reviewed_index = headers.index('reviewed')
        active_index = headers.index('active')
        # Apply conditional formatting for reviewed == 1 (light grey)
        sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=sheet_id,
            body={
                "requests": [
                    {
                        "addConditionalFormatRule": {
                            "rule": {
                                "ranges": [
                                    {
                                        "sheetId": 0,
                                        "startRowIndex": 1,  # Skip header row
                                        "endRowIndex": len(state_data) + 1,
                                        "startColumnIndex": 0,
                                        "endColumnIndex": len(headers)
                                    }
                                ],
                                "booleanRule": {
                                    "condition": {
                                        "type": "CUSTOM_FORMULA",
                                        "values": [
                                            {"userEnteredValue": f"=INDIRECT(ADDRESS(ROW(), {reviewed_index + 1}))=1"}
                                        ]
                                    },
                                    "format": {
                                        "backgroundColor": {"red": 0.6, "green": 0.6, "blue": 0.6}  # Light grey
                                    }
                                }
                            },
                            "index": 0
                        }
                    },
                ]
            }
        ).execute()
        # sheets_service.spreadsheets().batchUpdate(
        #     spreadsheetId=sheet_id,
        #     body={
        #         "requests": [
        #             # Apply conditional formatting for active == 0 (dark grey)
        #             {
        #                 "addConditionalFormatRule": {
        #                     "rule": {
        #                         "ranges": [
        #                             {
        #                                 "sheetId": 0,
        #                                 "startRowIndex": 1,  # Skip header row
        #                                 "endRowIndex": len(state_data) + 1,
        #                                 "startColumnIndex": 0,
        #                                 "endColumnIndex": len(headers)
        #                             }
        #                         ],
        #                         "booleanRule": {
        #                             "condition": {
        #                                 "type": "CUSTOM_FORMULA",
        #                                 "values": [
        #                                     {"userEnteredValue": f"=INDIRECT(ADDRESS(ROW(), {active_index + 1}))=FALSE"}
        #                                 ]
        #                             },
        #                             "format": {
        #                                 "backgroundColor": {"red": 0.6, "green": 0.6, "blue": 0.6}  # Dark grey
        #                             }
        #                         }
        #                     },
        #                     "index": 1
        #                 }
        #             }
        #         ]
        #     }
        # ).execute()
        print("Applied conditional formatting for reviewed and active columns.")

        # =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  =  = 

        # Apply filter view to all columns and rows with data
        sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=sheet_id,
            body={
                "requests": [
                    {
                        "setBasicFilter": {
                            "filter": {
                                "range": {
                                    "sheetId": 0,  # Typically the first sheet
                                    "startRowIndex": 0,  # Start from header row
                                    "endRowIndex": len(state_data) + 1,  # Adjust to cover all data rows
                                    "startColumnIndex": 0,
                                    "endColumnIndex": len(headers)
                                }
                            }
                        }
                    }
                ]
            }
        ).execute()
        print("Applied filter views to all columns, capturing all data rows.")


    except HttpError as e:
        print("An error occurred while building the sheet:", e)


def add_onEdit_script(sheet_name, shared_folder_id=shared_folder_id_global):
    # Step 1: Retrieve the spreadsheet ID based on the sheet name within the specified folder
    try:
        # Step 1: Check if the file already exists in the shared folder
        query = f"'{shared_folder_id}' in parents and name='{sheet_name}' and mimeType='application/vnd.google-apps.spreadsheet'"
        results = drive_service.files().list(
            q=query,
            fields="files(id, name)",
            includeItemsFromAllDrives=True,
            supportsAllDrives=True
        ).execute()
        files = results.get('files', [])
        
        if not files:
            print(f"No spreadsheet found with the name: {sheet_name} in folder: {shared_folder_id}")
            return
        spreadsheet_id = files[0]['id']
        print(f"Found spreadsheet '{sheet_name}' with ID: {spreadsheet_id}")


        # Apps Script code to add an onEdit function to the spreadsheet
        script_code = """
function onEdit(e) {
  const TIMESTAMP_HEADER = "last_updated";
  const REVIEWER_HEADER = "last_reviewer";
  const REVIEWED_HEADER = "reviewed";
  
  const sheet = e.source.getActiveSheet();
  const editedRange = e.range;

  // Find the columns for timestamp, reviewer, and reviewed
  const timestampColumn = findColumnByHeader(sheet, TIMESTAMP_HEADER);
  const reviewerColumn = findColumnByHeader(sheet, REVIEWER_HEADER);
  const reviewedColumn = findColumnByHeader(sheet, REVIEWED_HEADER);
  
  if (!timestampColumn || !reviewerColumn || !reviewedColumn) return;  // Exit if any column is missing

  // Only update if the edited column is not the timestamp, reviewer, or reviewed column
  if (editedRange.columnStart !== timestampColumn && editedRange.columnStart !== reviewerColumn) {
    const email = Session.getActiveUser().getEmail();  // Get the user's email once

    // Loop through each row in the edited range
    for (let i = 0; i < editedRange.getNumRows(); i++) {
      const row = editedRange.getRow() + i;
      
      // Update timestamp
      const timestampCell = sheet.getRange(row, timestampColumn);
      timestampCell.setValue(new Date());

      // Update reviewer
      const reviewerCell = sheet.getRange(row, reviewerColumn);
      reviewerCell.setValue(email);

      // Check the "reviewed" column and set the background color accordingly
      const reviewedCell = sheet.getRange(row, reviewedColumn);
      const reviewedValue = reviewedCell.getValue();
      
      if (reviewedValue === 1) {
        // Set background to light grey for reviewed == 1
        sheet.getRange(row, 1, 1, sheet.getLastColumn()).setBackgroundRGB(153, 153, 153);
      } else if (reviewedValue === 0) {
        // Set background to white for reviewed == 0
        sheet.getRange(row, 1, 1, sheet.getLastColumn()).setBackgroundRGB(255, 255, 255);
      }
    }
  }
}

function findColumnByHeader(sheet, headerName) {
  const headers = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0];
  const headerIndex = headers.indexOf(headerName);
  return headerIndex > -1 ? headerIndex + 1 : null;
}

        """

        # Step 2: Create a new Apps Script project bound to the Google Sheet
        request = {
            "title": "onEdit Script for " + sheet_name,
            "parentId": spreadsheet_id
        }
        response = script_service.projects().create(body=request).execute()
        project_id = response["scriptId"]

        # Step 3: Update the Apps Script project with the onEdit function
        request = {
            "files": [
                {
                    "name": "Code",
                    "type": "SERVER_JS",
                    "source": script_code
                }
            ]
        }
        script_service.projects().updateContent(
            scriptId=project_id, body=request
        ).execute()

        # Step 4: Deploy the Apps Script project
        deployment_config = {
            "manifestFileName": "appsscript",
            "deploymentConfig": {
                "versionNumber": 1,
                "manifestFileName": "appsscript"
            }
        }
        script_service.projects().deployments().create(
            scriptId=project_id, body=deployment_config
        ).execute()

        print(f"Added onEdit script to spreadsheet {spreadsheet_id}")
    
    except HttpError as e:
        print("An error occurred:", e)



# # # # # # # #  
# RUN
# # # # # # # #
for state in states:
# for state in ['AL']:
    print(f'starting {state}')

    # IMPORT
    # state_data_from_google = google_utils.pull_data(sheetname=state, folder=os.environ['PATH_TO_GOOGLE_CREDS'])
    # if state_data_from_google is None:
        # continue

    state_data_from_db = officials_state[officials_state['state'] == state][columns]

    exit()
    # google_utils.push_data(sheetname=state, folder=os.environ['PATH_TO_GOOGLE_CREDS'], data=state_data_from_db)
    # print(f'---------------done----------------')
    # time.sleep(15)
