import os, json, urllib.request, time, sys
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

value_map = {
    'party': {
        'Democratic': 'Democrat',
        'Republican': 'Republican',
    },
}

relevant_columns = [
    "name",
    "reviewed",
    "first_name",
    "last_name",
    "gender",
    "party",
    "email",
    "position",
    "district",
    "campaign_website",
    "government_website",
    "twitter_handle",
    "facebook",
    "instagram",
    "linkedin",
    "youtube",
    "truth_social",
    "tiktok",
    "last_reviewer",
    "id", # <-- KEEP THIS LAST!!! ALWAYS!!!
]

# # # # # # # # #
# RUN UPDATE FOR ALL STATES
# # # # # # # # #
if __name__ == '__main__': 
    # for state in ['HI']:
    for state in states:
        
        print(f'---{state}---')

        # PULL DATA AND CLEAN VALUES
        # ------------------------------------
        print('PULLING SPREADSHEET DATA')
        state_data_from_sheets = google_utils.pull_data(sheetname=state, folder=os.environ['STATE_DATA_CURATION_FOLDER'])
        
        if state_data_from_sheets is not None:
            state_data_from_sheets = (
                state_data_from_sheets
                .replace({
                    '': None,
                    ' ': None,
                    'null': None,
                    'None': None,
                })
                .drop(columns=['name'])
            )


            for col in value_map:
                print(f'\tconverting values from column {col}')
                state_data_from_sheets[col] = state_data_from_sheets[col].apply(lambda x: value_map[col].get(x, x))

            if "--skip-pull-from-sheets" not in sys.argv: # <-- allow caller to skip this
                # SEND DATA TO DB
                # ------------------------------------
                print('PUSHING SPREADSHEET DATA TO DB')
                dbx = dataset.connect(params)
                dbx['officials'].update_many(
                    state_data_from_sheets.to_dict(orient = 'records'),
                    'id'
                )
                dbx.engine.dispose(); dbx.close()

                print('\t___')
                time.sleep(5) 
            else:
                print('SKIPPING PULL FROM SHEETS!!')

            # PUSH BACK TO SHEETS
            # ------------------------------------
            print('PUSHING STATE DATA FROM DB TO SHEETS; ')
            state_data_from_db = (
                db.table('officials')
                .filter([_.state == state, _.active == 1, _.level == 'state'])
                .order_by([_.reviewed, _.position, _.name])
                .execute()
                .replace({
                    np.nan: '',
                    pd.NaT: '',
                    None: '',
                })
            )[relevant_columns]

            google_utils.push_data(sheetname = state, folder = os.environ['STATE_DATA_CURATION_FOLDER'], data = state_data_from_db)
            print('________pushed________')
            time.sleep(5) 





