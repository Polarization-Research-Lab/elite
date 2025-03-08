import os, json, urllib.request, time
from datetime import datetime

import dotenv
import ibis; from ibis import _
import dataset
import numpy as np 
import pandas as pd
import json5

import google_utils

# # # # # # # #  
# SETUP
# # # # # # # # 
dotenv.load_dotenv('../../../env'); dotenv.load_dotenv(os.environ['PATH_TO_SECRETS'])

params = f"{os.environ['DB_DIALECT']}://{os.environ['DB_USER']}:{urllib.parse.quote(os.environ['DB_PASSWORD'])}@localhost:{os.environ['DB_PORT']}/elite"
db = ibis.mysql.connect(
    host=os.environ['DB_HOST'],
    user=os.environ['DB_USER'],
    password=os.environ['DB_PASSWORD'],
    database='elite',
)

states = (
    db.table('officials')
    .group_by(_.state)
    .aggregate()
    .execute()
    ['state']
    .to_list()
)

officials = (
    db.table('officials')
    .filter([_.level == 'state', _.active == 1])
)


# Connect to the Google Drive and Sheets APIs
drive_service = google_utils.drive_service
sheets_service = google_utils.sheets_service

states_drive_folder = os.environ['STATE_DATA_CURATION_FOLDER']

def delete_these_columns(sheetname, folder):
    try:
        # Step 1: Check if the file already exists in the shared folder
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
            return
        spreadsheet_id = files[0]['id']
        print(f"Found spreadsheet '{sheetname}' with ID: {spreadsheet_id}")

        # Step 2: Retrieve the first sheet and headers
        sheet_metadata = sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        sheet_id = sheet_metadata['sheets'][0]['properties']['sheetId']  # Assuming first sheet
        headers = sheets_service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range="A1:ZZ1"  # Assuming headers are in the first row
        ).execute().get('values', [[]])[0]

        # Step 3: Find the index of the "id" column
        try:
            serving_public_since = headers.index("serving_public_since")
            birthday = headers.index("birthday")
        except ValueError:
            print("Columns not found in headers.")
            return

        # Step 4: delte the columns
        requests = [
            {
                "deleteDimension": {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": "COLUMNS",
                        "startIndex": serving_public_since,  # Start at the index of "id" column
                        "endIndex": serving_public_since + 1  # End at the next column
                    },
                }
            },
            {
                "deleteDimension": {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": "COLUMNS",
                        "startIndex": birthday,  # Start at the index of "id" column
                        "endIndex": birthday + 1  # End at the next column
                    },
                }
            },
        ]

        sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": requests}
        ).execute()

        print(f"Column deleted successfully in spreadsheet '{sheetname}'.")

    except google_utils.HttpError as e:
        print("An error occurred:", e)
        return None

def delete_this_column(column, sheetname, folder):
    try:
        # Step 1: Check if the file already exists in the shared folder
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
            return
        spreadsheet_id = files[0]['id']
        print(f"Found spreadsheet '{sheetname}' with ID: {spreadsheet_id}")

        # Step 2: Retrieve the first sheet and headers
        sheet_metadata = sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        sheet_id = sheet_metadata['sheets'][0]['properties']['sheetId']  # Assuming first sheet
        headers = sheets_service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range="A1:ZZ1"  # Assuming headers are in the first row
        ).execute().get('values', [[]])[0]

        # Step 3: Find the index of the "id" column
        try:
            column_index = headers.index(column)
        except ValueError:
            print("Column not found in headers.")
            return

        # Step 4: delte the columns
        requests = [
            {
                "deleteDimension": {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": "COLUMNS",
                        "startIndex": column_index,  # Start at the index of "id" column
                        "endIndex": column_index + 1  # End at the next column
                    },
                }
            },
        ]

        sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": requests}
        ).execute()

        print(f"Column deleted successfully in spreadsheet '{sheetname}'.")

    except google_utils.HttpError as e:
        print("An error occurred:", e)
        return None

def replace_these_columns(sheetname, folder):
    try:
        # Step 1: Retrieve the spreadsheet ID
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
        print(f"Found spreadsheet '{sheetname}' with ID: {spreadsheet_id}")

        # Step 2: Pull Google Sheets data
        state_data_google = google_utils.pull_data(sheetname, folder)

        # Create the new identifier column in the Google Sheets data
        state_data_google['name_district_identifier'] = state_data_google.apply(
            lambda x: f"{x['name']}-{x['district']}", axis=1
        )

        # Step 3: Filter database data for the given state and add the new identifier column
        state_data_database = (
            officials.filter(_.state == sheetname)
            .select('id', 'facebook', 'name', 'district')
            .mutate(name_district_identifier=_.name + "-" + _.district)  # Create the identifier in the database data
            .execute()
        )

        # Step 4: Sanity Check for Alignment on `name_district_identifier`
        # Identify mismatched identifiers
        google_identifiers = set(state_data_google['name_district_identifier'])
        database_identifiers = set(state_data_database['name_district_identifier'])

        # Check for identifiers in Google Sheets but not in the database
        missing_in_database = google_identifiers - database_identifiers
        # Check for identifiers in the database but not in Google Sheets
        missing_in_google = database_identifiers - google_identifiers

        if missing_in_database or missing_in_google:
            print("Sanity Check Failed!")
            if missing_in_database:
                print("The following identifiers are in Google Sheets but not in the database:")
                print(missing_in_database)
            if missing_in_google:
                print("The following identifiers are in the database but not in Google Sheets:")
                print(missing_in_google)
            print("Exiting script due to unmatched rows.")
            exit()

        # Step 5: Align rows and update the `facebook` column
        # Update the `id` column in state_data_google using the mapping
        id_mapping = state_data_database.set_index('name_district_identifier')['id']
        state_data_google['id'] = state_data_google['name_district_identifier'].map(id_mapping)

        # Update the `facebook` column in state_data_google only if the current value is None or ''
        facebook_mapping = state_data_database.set_index('name_district_identifier')['facebook']
        updated_facebook = state_data_google['name_district_identifier'].map(facebook_mapping)
        state_data_google['facebook'] = state_data_google['facebook'].where( # <-- Use .where to preserve existing values if they're not None or ''
            state_data_google['facebook'].notna() & (state_data_google['facebook'] != ''), 
            updated_facebook
        )

        # Debugging prints to validate alignment
        # print(state_data_google[['id', 'name_district_identifier', 'facebook']].head())
        # print(state_data_database[['id', 'name_district_identifier', 'facebook']].head())
        # print(state_data_google['id'].isnull().sum(), 'missing ids. sanity checks passed. sending:')

        # Step 6: Push updated data back to the Google Sheet
        google_utils.push_data(sheetname, folder, state_data_google[['id','facebook']])
        print(f"Columns replaced successfully in spreadsheet '{sheetname}'.")

    except google_utils.HttpError as e:
        print("An error occurred:", e)
        return None

def set_column_to_datetime_format(column, sheetname, folder):
    try:
        # Step 1: Check if the file already exists in the shared folder
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
            return
        spreadsheet_id = files[0]['id']
        print(f"Found spreadsheet '{sheetname}' with ID: {spreadsheet_id}")

        # Step 2: Retrieve the first sheet and headers
        sheet_metadata = sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        sheet_id = sheet_metadata['sheets'][0]['properties']['sheetId']  # Assuming first sheet
        headers = sheets_service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range="A1:ZZ1"  # Assuming headers are in the first row
        ).execute().get('values', [[]])[0]

        # Step 3: Find the index of the specified column
        try:
            column_index = headers.index(column)
        except ValueError:
            print("Column not found in headers.")
            return

        # Step 4: Set the column format to datetime
        requests = [
            {
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": 1,  # Skip the header row
                        "endRowIndex": None,  # Apply to all rows
                        "startColumnIndex": column_index,
                        "endColumnIndex": column_index + 1,
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "numberFormat": {
                                "type": "DATE_TIME",
                                "pattern": "yyyy-MM-dd HH:mm:ss"
                            }
                        }
                    },
                    "fields": "userEnteredFormat.numberFormat"
                }
            }
        ]

        sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": requests}
        ).execute()

        print(f"Column '{column}' set to datetime format successfully in spreadsheet '{sheetname}'.")

    except google_utils.HttpError as e:
        print("An error occurred:", e)
        return None

def delete_all_appscripts(sheetname, folder):
    try:
        # Step 1: Find the spreadsheet
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
            return
        spreadsheet_id = files[0]['id']
        print(f"Found spreadsheet '{sheetname}' with ID: {spreadsheet_id}")

        # Step 2: List all Apps Script projects attached to the spreadsheet
        scripts = script_service.projects().list(
            scriptId=spreadsheet_id
        ).execute()

        if not scripts.get("projects"):
            print(f"No Apps Script projects attached to '{sheetname}'.")
            return

        # Step 3: Delete each Apps Script project
        for script in scripts["projects"]:
            script_id = script["scriptId"]
            print(f"Deleting Apps Script project with ID: {script_id}")
            script_service.projects().delete(scriptId=script_id).execute()

        print(f"All Apps Script projects have been deleted from '{sheetname}'.")

    except google_utils.HttpError as e:
        print(f"An error occurred while deleting Apps Scripts: {e}")
        return None

if __name__ == '__main__':
    
    print('Remove this to actually run (leave them commented out unless activily using this script); Exiting...')
    exit()

    for state in states:
        print(f'performing migration for {state}')
        # delete_these_columns(sheetname=state, folder=states_drive_folder)
        # delete_this_column(column='twitter_id',sheetname=state, folder=states_drive_folder)
        # replace_these_columns(sheetname=state, folder=states_drive_folder)
        # delete_this_column(column='serving_position_since',sheetname=state, folder=states_drive_folder)
        # delete_this_column(column='last_updated',sheetname=state, folder=states_drive_folder)
        # set_column_to_datetime_format(column='last_updated',sheetname=state, folder=states_drive_folder)
        # delete_this_column(column='active',sheetname=state, folder=states_drive_folder)
        # delete_this_column(column='suffix',sheetname=state, folder=states_drive_folder)
        # delete_this_column(column='middle_name',sheetname=state, folder=states_drive_folder)
        # delete_this_column(column='nick_name',sheetname=state, folder=states_drive_folder)
        print('-----------done---------')
        # exit()
        time.sleep(5)
