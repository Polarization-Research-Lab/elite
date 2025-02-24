import json, io, os, urllib
import pandas as pd
import numpy as np
import requests
import dataset
import dotenv
import yaml


# DB Connection
dotenv.load_dotenv('../../env')
dotenv.load_dotenv(os.environ['PATH_TO_SECRETS'])

db = f"{os.environ['DB_DIALECT']}://{os.environ['DB_USER']}:{urllib.parse.quote(os.environ['DB_PASSWORD'])}@localhost:{os.environ['DB_PORT']}/elite"

## get officials
dbx = dataset.connect(db)
officials = pd.DataFrame(dbx['officials'].find(level = 'national')).replace({np.nan: None})
dbx.engine.dispose(); dbx.close()

# Load data sources
with open('unitedstates/congress-legislators/legislators-current.yaml', 'r') as f:
    legislators_current = yaml.safe_load(f)

with open('unitedstates/congress-legislators/legislators-social-media.yaml', 'r') as f:
    legislators_social_media = yaml.safe_load(f)

# # # # # # # # # # # # # # 
# Update Old People
# # # # # # # # # # # # # # 
print('UPDATED EXISTING LEGISLATORS')
ids_for_internal_data = officials['bioguide_id'].to_list()

current_reformatted = {l['id']['bioguide']: l for l in legislators_current}
current_reformatted_socials = {l['id']['bioguide']: l for l in legislators_social_media}

# UPDATE LEGISLATORS TABLE
def updated_from_unitedstates(official):
    """
    Updates an official's information using the `current_reformatted` dictionary.
    Returns:
        dict: A dictionary with updated data for the official.
    """
    # Check for legislator in current_reformatted
    bioguide_id = official.get('bioguide_id')
    legislator = current_reformatted.get(bioguide_id, {})
    if legislator:    
        new_data = {
            'first_name': legislator.get('name', {}).get('first'),
            'last_name': legislator.get('name', {}).get('last'),
            'middle_name': legislator.get('name', {}).get('middle'),
            'nick_name': legislator.get('name', {}).get('nickname'),
            'gender': {'M': 'man', 'F': 'woman'}.get(legislator.get('bio', {}).get('gender')),
            'state': legislator.get('terms', [{}])[-1].get('state'),
            'party': legislator.get('terms', [{}])[-1].get('party'),
            'government_website': legislator.get('terms', [{}])[-1].get('url'),
            'level': 'national',
            'active': 1,
            'district': legislator.get('terms', [{}])[-1].get('district'),
            'position': {'rep': 'Representative', 'sen': 'Senator'}.get(legislator.get('terms', [{}])[-1].get('type')),
            'type': {'rep': 'Representative', 'sen': 'Senator'}.get(legislator.get('terms', [{}])[-1].get('type')), # <-- redundant but needed  for backwards compatibility
            'bioguide_id': bioguide_id,
            'fec_ids': ','.join(legislator['id'].get('fec')) if legislator['id'].get('fec') else None,
            'federal': {
                'senate_class': legislator.get('terms', [{}])[-1].get('class'),
            },
        }

        # Check for legisaltor in social medias
        legislator_socials = current_reformatted_socials.get(bioguide_id, {})
        if legislator_socials:
            new_data['facebook'] = legislator_socials['social'].get('facebook')
            new_data['youtube'] = legislator_socials['social'].get('youtube')
            new_data['youtube_id'] = legislator_socials['social'].get('youtube_id')
            new_data['instagram'] = legislator_socials['social'].get('instagram')

            # Update Twitter ID conditionally
            twitter_id = legislator_socials['social'].get('twitter_id')
            if twitter_id:
                existing_twitter_ids = official.get('twitter_id')
                if existing_twitter_ids:
                    # Check if the new twitter_id is already in the list
                    if str(int(twitter_id)) not in existing_twitter_ids:
                        new_data['twitter_id'] = f"{existing_twitter_ids},{twitter_id}"
                else:
                    new_data['twitter_id'] = twitter_id  # First entry

        new_data = {item: new_data[item] for item in new_data if new_data[item]}

    else:
        new_data = {
            'bioguide_id': bioguide_id,
            'active': 0,
        }

    return new_data

officials_updates = officials.apply(
    lambda official: updated_from_unitedstates(official),
    axis=1
).to_list()

# !!! NOTE !!! Use "upsert" instead of "update" otherwise dataset will fill missing columns with null
dbx = dataset.connect(db)
dbx['officials'].upsert_many(
    officials_updates,
    'bioguide_id',
)
dbx.engine.dispose(); dbx.close()
print('\t...upserted.')




# # # # # # # # # # # # # # 
# Add New People
# # # # # # # # # # # # # # 
print('INSERTING NEW LEGISLATORS')
new_people = []
for legislator_bioguide_id in current_reformatted:
    if legislator_bioguide_id not in officials['bioguide_id'].to_list():
        legislator = current_reformatted[legislator_bioguide_id]
        legislator_socials = current_reformatted_socials[legislator_bioguide_id]

        new_people.append({
            'first_name': legislator.get('name', {}).get('first'),
            'last_name': legislator.get('name', {}).get('last'),
            'middle_name': legislator.get('name', {}).get('middle'),
            'nick_name': legislator.get('name', {}).get('nickname'),
            'gender': {'M': 'man', 'F': 'woman'}.get(legislator.get('bio', {}).get('gender')),
            'state': legislator.get('terms', [{}])[-1].get('state'),
            'party': legislator.get('terms', [{}])[-1].get('party'),
            'government_website': legislator.get('terms', [{}])[-1].get('url'),
            'level': 'national',
            'active': 1,
            'district': legislator.get('terms', [{}])[-1].get('district'),
            'position': {'rep': 'Representative', 'sen': 'Senator'}.get(legislator.get('terms', [{}])[-1].get('type')),
            'type': {'rep': 'Representative', 'sen': 'Senator'}.get(legislator.get('terms', [{}])[-1].get('type')), # <-- redundant but needed  for backwards compatibility
            'bioguide_id': legislator['id']['bioguide'],
            'fec_ids': legislator['id'].get('fec'),
            'federal': {
                'senate_class': legislator.get('terms', [{}])[-1].get('class'),
            },
            'facebook': legislator_socials['social'].get('facebook'),
            'youtube': legislator_socials['social'].get('youtube'),
            'youtube_id': legislator_socials['social'].get('youtube_id'),
            'instagram': legislator_socials['social'].get('instagram'),
            'twitter_id': legislator_socials['social'].get('twitter_id'),
        })
        print(new_people)
        exit()

if new_people:
    dbx = dataset.connect(db)
    dbx['officials'].insert_many(
        new_people,
    )
    dbx.engine.dispose(); dbx.close()
    print('\t...inserted.')
else:
    print('\t...nothing to insert.')
