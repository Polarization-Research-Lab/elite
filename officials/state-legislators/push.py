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
                    row_as_dict[col] = official_value # Add the updated value

            # Treat Nones as empty strings for comparison
            elif str(row_value or '') != str(official_value or ''):
                # print(col, row_value, '|', official_value)
                row_as_dict[col] = official_value  # Add the updated value

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

    # Identify rows in officials_state not present in state_data
    missing_rows = officials_state[
        (officials_state['state'] == state) & 
        (~officials_state['id'].astype(int).isin(state_data['id'].astype(int)))
    ]

    # Filter missing rows to include only columns in state_data
    filtered_missing_rows = missing_rows[state_data.columns]

    # Concatenate filtered missing rows to state_data while preserving column order
    state_data = pd.concat([state_data, filtered_missing_rows], ignore_index=True)

    # print(updated)
    df1 = state_data.copy()
    for updated_item in updated:
        # Get the id of the current updated item
        updated_id = updated_item.get('id')
        if not updated_id:
            continue  # Skip if there's no id in the updated item

        # Find the corresponding row in state_data based on the id
        row_index = state_data[state_data['id'] == updated_id].index

        if not row_index.empty:
            # Update each column in the state_data row with the values from updated_item
            for key, value in updated_item.items():
                # print(f"updating {key}, {value} @ {row_index}")
                state_data.at[row_index[0], key] = value  # Use `.at` for scalar updates

    # Send the updated state_data back to Google Sheets
    df2 = state_data.copy()
    if (len(updated) > 0) | (missing_rows.shape[0] > 0):

        discrepancies = []
        for row_idx, col in zip(*((df1 != df2).values.nonzero())):
            discrepancies.append({
                'row': row_idx,
                'column': df1.columns[col],
                'df1_value': df1.iloc[row_idx, col],
                'df2_value': df2.iloc[row_idx, col]
            })

        # Print discrepancies
        for d in discrepancies:
            print('\t',d)

        state_data = state_data.replace({np.nan: None})
        google_utils.push_data(
            state, 
            '1K-CohXS8oeaqXbOr5UVkG908fpquMNBP', 
            state_data
        )
        time.sleep(30)  # Avoid rate limit
    else:
        print('!!!!!!!!!!!!!!!NO UPDATES!!!!!!!!!!!!!!')
        time.sleep(5)

    print('---[done]---\n\n')


    # print(f"Processed updates and inserts for state: {state}")
