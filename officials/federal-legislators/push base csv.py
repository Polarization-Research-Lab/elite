import json, io, os, urllib
import pandas as pd
import numpy as np
import requests
import dataset
import dotenv


# DB Connection
dotenv.load_dotenv('../../env')
dotenv.load_dotenv(os.environ['PATH_TO_SECRETS'])
db = f"{os.environ['DB_DIALECT']}://{os.environ['DB_USER']}:{urllib.parse.quote(os.environ['DB_PASSWORD'])}@localhost:{os.environ['DB_PORT']}/elite"

## get officials
dbx = dataset.connect(db)
officials = pd.DataFrame(dbx['officials'].find(level = 'national', active = True))
dbx.engine.dispose(); dbx.close()

# Get Data sources
github_table = pd.read_csv('.tmp/legislators-current.csv')

## use a more complete set of twitter handles
with open('.tmp/legislators-social.json', 'r') as file:
    socials_json = json.load(file)


# New Peoples
ids_for_internal_data = officials['bioguide_id'].to_list()

new = github_table.apply(lambda x: x['bioguide_id'] in ids_for_internal_data, axis = 1)
new = github_table[new == False]

submit_new = []
new = new.replace({np.nan: None})
for person in new.to_dict(orient = 'records'):
    submit_new.append({
        'first_name': person.get('first_name'),
        'last_name': person.get('last_name'),
        'middle_name': person.get('middle_name'),
        'nick_name': person.get('nickname'),
        'gender': {'M': 'man', 'F': 'woman'}.get(person['gender']),
        'state': person.get('state'),
        'party': person.get('party'),
        'government_website': person.get('url'),
        'twitter_id': socials_json.get(person['bioguide_id'], {}).get('twitter_id'),
        'facebook': person.get('facebook'),
        'instagram': person.get('instagram'),
        'youtube': person.get('youtube_id'),
        'level': 'national',
        'active': True,
        'district': person.get('district'),
        'type': {'rep': 'Representative', 'sen': 'Senator'}.get(person['type']),
        'position': {'rep': 'Representative', 'sen': 'Senator'}.get(person['type']),
        'bioguide_id': person.get('bioguide_id'),
        'federal': {
            'senate_class': person.get('senate_class'),
        },
    })

dbx = dataset.connect(db)
dbx['officials'].upsert_many(
    submit_new,
    keys=['bioguide_id'], 
)
dbx.engine.dispose(); dbx.close()
print('Sent new profiles to database')


# Update Old People
ids_for_current_legislators = github_table['bioguide_id'].to_list()

## Are they still active? if they aren't in the github table then no
officials['active'] = officials.apply(lambda x: x['bioguide_id'] in ids_for_current_legislators, axis = 1)

# For 'federal' with 'senate_class'
officials['federal'] = officials.apply(lambda x: 
    {'senate_class': github_table.loc[github_table['bioguide_id'] == x['bioguide_id'], 'senate_class'].values[0]
     if not github_table.loc[github_table['bioguide_id'] == x['bioguide_id'], 'senate_class'].empty else None}, axis=1)

# Replace any NaN values in 'federal' with None
officials['federal'] = officials['federal'].apply(lambda val: {k: (None if pd.isna(v) else v) for k, v in val.items()})

# For 'fec_ids'
officials['fec_ids'] = officials.apply(lambda x: github_table.loc[github_table['bioguide_id'] == x['bioguide_id'], 'fec_ids'].values[0] if not github_table.loc[github_table['bioguide_id'] == x['bioguide_id'], 'fec_ids'].empty else None, axis=1)

# For 'birthday'
officials['birthday'] = officials.apply(lambda x: github_table.loc[github_table['bioguide_id'] == x['bioguide_id'], 'birthday'].values[0] if not github_table.loc[github_table['bioguide_id'] == x['bioguide_id'], 'birthday'].empty else None, axis=1)

## Send of to database
officials = officials.replace({np.nan: None})

dbx = dataset.connect(db)
dbx['officials'].upsert_many(
    officials.to_dict(orient='records'), 
    keys=['bioguide_id'], 
)
dbx.engine.dispose(); dbx.close()
print('Sent updated profile data to database')
