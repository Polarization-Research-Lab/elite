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

params_ops = f"{os.environ['DB_DIALECT']}://{os.environ['DB_USER']}:{urllib.parse.quote(os.environ['DB_PASSWORD'])}@localhost:{os.environ['DB_PORT']}/operations"
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


# # # # # # # # #
# RUN VERSION CHECK
# # # # # # # # #
if __name__ == '__main__': 
    # for state in ['NH']:  # Test with one state for now
    for state in states:  # Test with one state for now
        
        print(f'---saving activity for: {state}---')

        # GET VERSION HISTORY (update time and name of updater)
        # ------------------------------------
        version_history = google_utils.get_version_history(sheetname=state, folder=os.environ['STATE_DATA_CURATION_FOLDER'])
        if version_history:
            for v in range(len(version_history)):
                version_history[v]['state'] = state
                version_history[v]['unique_id'] = f"{state}-{version_history[v]['revision_id']}"

            version_history = [v for v in version_history if v['editor'] != os.environ['GOOGLE_API_AGENT']]

            # SEND DATA TO DB
            # ------------------------------------
            print('\tPUSHING ACTIVITY TO TO DB')
            dbx = dataset.connect(params_ops)
            dbx['state_data_curation'].upsert_many(
                version_history,
                'unique_id'
            )
            dbx.engine.dispose(); dbx.close()

        else:
            print(f"None found for {state}")
        print('\t___')
        time.sleep(5) 

