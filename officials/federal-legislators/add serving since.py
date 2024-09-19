import json, time, urllib
from datetime import datetime
import pandas as pd
import numpy as np
import dataset
import os
import dotenv


# DB Connection
dotenv.load_dotenv('../env')
dotenv.load_dotenv(os.environ['PATH_TO_SECRETS'])
db = f"{os.environ['DB_DIALECT']}://{os.environ['DB_USER']}:{urllib.parse.quote(os.environ['DB_PASSWORD'])}@localhost:{os.environ['DB_PORT']}/elite"

## Access the legislators table
dbx = dataset.connect(db)
officials = pd.DataFrame(dbx['officials'].find(level = 'national', active = True))
dbx.engine.dispose(); dbx.close()

## Make columns if they dont exist
with dataset.connect(db) as dbx:
    dbx['officials'].create_column('serving_public_since', dbx.types.datetime, nullable = True)
    dbx['officials'].create_column('serving_position_since', dbx.types.datetime, nullable = True)

# Load Assets
with open('.tmp/legislators-current.json', 'r') as file:
    github_data = json.load(file)

# Update serving since columns for each legislator
for legislator in github_data:
    bioguide_id = legislator['id']['bioguide']

    terms = legislator['terms']

    # get serving since and serving chamber since
    current_date = datetime.now().date()
    serving_since = ""
    serving_current_chamber_since = ""
    current_chamber = terms[-1]['type']  # The type of the last term represents the current chamber

    for term in terms:
        term_start = datetime.strptime(term['start'], "%Y-%m-%d").date()
        
        # For "serving since", find the earliest start date
        if serving_since == "" or term_start < datetime.strptime(serving_since, "%Y-%m-%d").date():
            serving_since = term['start']
        
        # For "serving current chamber since", find the start date of the current chamber type
        if term['type'] == current_chamber:
            if serving_current_chamber_since == "" or term_start < datetime.strptime(serving_current_chamber_since, "%Y-%m-%d").date():
                serving_current_chamber_since = term['start']

    # sendit
    dbx = dataset.connect(db)
    dbx['officials'].update(
        {
            'bioguide_id': bioguide_id,
            'serving_public_since': serving_since,
            'serving_position_since': serving_current_chamber_since,
        }, 
        ['bioguide_id']
    )
    dbx.engine.dispose(); dbx.close()
