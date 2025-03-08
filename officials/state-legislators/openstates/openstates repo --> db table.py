import os, json, urllib.request, time
from datetime import datetime

import dotenv
import ibis; from ibis import _
import dataset
import numpy as np 
import pandas as pd
import json5
import yaml
import orjson

# # # # # # # #  
# SETUP
# # # # # # # # 
dotenv.load_dotenv('../../../env'); dotenv.load_dotenv(os.environ['PATH_TO_SECRETS'])

params = f"{os.environ['DB_DIALECT']}://{os.environ['DB_USER']}:{urllib.parse.quote(os.environ['DB_PASSWORD'])}@localhost:{os.environ['DB_PORT']}/elite"
paramsops = f"{os.environ['DB_DIALECT']}://{os.environ['DB_USER']}:{urllib.parse.quote(os.environ['DB_PASSWORD'])}@localhost:{os.environ['DB_PORT']}/ops"
db = ibis.mysql.connect(
    host=os.environ['DB_HOST'],
    user=os.environ['DB_USER'],
    password=os.environ['DB_PASSWORD'],
    database='elite',
)

# Fetch the officials_state table structure and states list
officials_state = (
    db.table('officials')
    .filter([_.level == 'state'])
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

db.raw_sql("UPDATE openstates SET active = 0")

# Process each state data sheet
for state in states:
    if state:
        print(state)
        src = f'people/data/{state.lower()}/legislature/'
        entries = []
        if os.path.isdir(src):
            
            for file in os.listdir(src):
                if not os.path.isdir(os.path.join(src, file)):
                    with open(os.path.join(src,file), 'r') as filecontent:
                        legislator = yaml.safe_load(filecontent)

                    entries.append({
                        'openstates_id': legislator['id'],
                        'name': legislator['name'],
                        'state': state,
                        'openstates_data': json.dumps(json.loads(orjson.dumps(legislator).decode('utf-8'))),
                        'active': 1,
                    })

            # # Send updated records to the database
            if entries:
                dbx = dataset.connect(params)
                dbx['openstates'].upsert_many(
                    entries,
                    'openstates_id'
                )
                dbx.engine.dispose(); dbx.close()
        else:
            print(f'--- state {state} not found! ---')
