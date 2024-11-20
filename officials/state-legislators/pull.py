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
dotenv.load_dotenv('../../env'); dotenv.load_dotenv(os.environ['PATH_TO_SECRETS'])

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

# Process each state data sheet
for state in states:
    print(state)
    state_data = google_utils.pull_data(sheetname=state, folder='1K-CohXS8oeaqXbOr5UVkG908fpquMNBP')

    if state_data is None:
        continue  # Skip if no data was returned

    def update_check(row):
        if row['id'] in ['', None]:
            return {}  # Return empty dictionary for new rows without an ID

        # Find the corresponding row in officials_state by ID
        official_row = officials_state[officials_state['id'] == int(row['id'])]
        if official_row.empty:
            print(f"Error: ID {row['id']} not found in officials_state")
            exit(1)

        # Initialize dictionary with the 'id' and only include updated columns
        row_as_dict = {'id': row['id']}
        row['reviewed'] = int(row['reviewed'])

        for col in state_data.columns:
            row_value = row[col]
            official_value = official_row.iloc[0][col]

            # Convert only official_value to a date string if it's a datetime, to match row_value's format
            if isinstance(official_value, datetime) or (col in ['last_updated', 'serving_public_since', 'serving_position_since']):
                official_value = official_value.strftime('%Y-%m-%d %H:%M:%S') if official_value else None
                row_value = pd.to_datetime(row_value, errors='coerce').strftime('%Y-%m-%d %H:%M:%S') if row_value else None
                if official_value != row_value:
                    print(col,official_value, row_value)
                    row_as_dict[col] = row_value # Add the updated value

            # Treat Nones as empty strings for comparison
            elif str(row_value or '') != str(official_value or ''):
                # print(col, row_value, '|', official_value)
                row_as_dict[col] = row_value  # Add the updated value

        # Return only updated columns; empty dict if no changes
        return row_as_dict if len(row_as_dict) > 1 else {}

    # Apply update_check to get a list of dictionaries, one per row, with only the changed columns
    updated_rows = state_data.apply(update_check, axis=1).tolist()

    # Filter out empty dictionaries from updated_rows
    updated = [row for row in updated_rows if row]

    new = state_data[state_data['id'].isnull()]

    # print('ORIGINAL=====\n',state_data,'\n---------------------------\n')
    # print('UPDATED=====\n',len(updated),'\n---------------------------\n')
    # print('NEW=====\n',new,'\n---------------------------\n')
    print(state, new.shape[0], len(updated))
    # print(updated); exit()

    # # Send updated records to the database
    if len(updated) > 0:
        with dataset.connect(params) as dbx:
            for up in updated:
                dbx['officials'].update(
                    up,
                    'id',
                )

            for up in updated:
                dbx['curation'].insert(
                    {
                        'action': 'update',
                        'data': up,
                    }
                )
        print('SENT UPDATES')
    else: print('NO UPDATES??????')


    # Identify new records without an ID and insert them
    if not new.empty:
        with dataset.connect(params) as dbx:
            dbx['officials'].insert_many(
                new.to_dict(orient='records')
            )
            for n in new.to_dict(orient='records'):
                dbx['curation'].insert(
                    {
                        'action': 'insert',
                        'data': n,
                    }
                )
        print('SENT INSERTS')
    else: print('NO INSERTS.....')

    # print(f"Processed updates and inserts for state: {state}")
    time.sleep(1)  # Avoid rate limit
    # exit()
