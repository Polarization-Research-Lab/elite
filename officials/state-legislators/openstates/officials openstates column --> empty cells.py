import os, json, urllib.request, time, re
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
paramsops = f"{os.environ['DB_DIALECT']}://{os.environ['DB_USER']}:{urllib.parse.quote(os.environ['DB_PASSWORD'])}@localhost:{os.environ['DB_PORT']}/ops"
db = ibis.mysql.connect(
    host=os.environ['DB_HOST'],
    user=os.environ['DB_USER'],
    password=os.environ['DB_PASSWORD'],
    database='elite',
)
officials = db.table('officials').filter([_.level == 'state', _.reviewed == 0])
openstates = db.table('openstates').select([_.openstates_id,_.openstates_data])

# Fetch the officials_state table structure and states list
officials_state = (
    officials
    .join(
        openstates,  # Ensure `openstates` gets a unique alias
        officials.openstates_id == openstates.openstates_id
    )
    .execute()
)

states = (
    db.table('officials')
    .group_by(_.state)
    .aggregate()
    .execute()
    ['state']
    .to_list()
) 

print(officials_state.shape)
all_updates = []
party=[]
for o, official in officials_state.iterrows():
    openstates = json5.loads(official['openstates_data'])
    updates = {}

    updates['first_name'] = openstates.get('given_name')
    updates['last_name'] = openstates.get('family_name')
    updates['gender'] = openstates.get('gender')
    if updates['gender'] == 'Male': updates['gender'] = 'man'
    elif updates['gender'] == 'Female': updates['gender'] = 'woman'
    updates['email'] = openstates.get('email')
    updates['party'] = openstates.get('party',[{}])[0].get('name')
    updates['party'] = {
        'Democratic': 'Democrat',
        'Republican': 'Republican',
    }.get(updates['party'], updates['party'])
    party.append(updates['party'])

    # Should just use our RAs for this. We can get the openstates one with official.openstates_data
    # updates['twitter_handle'] = openstates.get('ids',{}).get('twitter')
    # updates['youtube'] = openstates.get('ids',{}).get('youtube')
    # updates['instagram'] = openstates.get('ids',{}).get('instagram')
    # updates['facebook'] = openstates.get('ids',{}).get('facebook')

    # district info
    roles = openstates.get('roles', [])
    if roles:
        earliest_role = min(
            roles, 
            key=lambda role: datetime.strptime(role['start_date'], '%Y-%m-%d') if 'start_date' in role else datetime.max
        )
        updates['position'] = earliest_role.get('type')
        updates['district'] = earliest_role.get('district')

    updates = {update: updates[update] for update in updates if updates[update] is not None}
    if updates:
        updates['id'] = official['id']
        all_updates.append(updates)

dbx = dataset.connect(params)
dbx['officials'].upsert_many(
    all_updates,
    'id',
)
dbx.engine.dispose(); dbx.close()
print('PUSHED UPDATES')



