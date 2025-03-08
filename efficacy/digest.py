import os, json, urllib, pickle, time

import dotenv
import pandas as pd
import requests
import dataset

dotenv.load_dotenv('../env')
dotenv.load_dotenv(os.environ['PATH_TO_SECRETS'])
params = f"{os.environ['DB_DIALECT']}://{os.environ['DB_USER']}:{urllib.parse.quote(os.environ['DB_PASSWORD'])}@localhost:{os.environ['DB_PORT']}/elite"

# DB TAble Build
tablename = 'efficacy'
with dataset.connect(params) as dbx:
    table = dbx.create_table(tablename, primary_id = 'id', primary_type = dbx.types.integer, primary_increment = True)
    table.create_column('bioguide_id', dbx.types.string(7), unique = True, nullable = False)
    table.create_column('sponsored', dbx.types.json)
    table.create_column('cosponsored', dbx.types.json)
    table.create_column('topics', dbx.types.json)


# Get Agg Stats
bills = pd.read_csv('.tmp/bills.csv')
# bills = pd.read_csv('.tmp/bills.csv')

sponsored_by_anyone = bills[bills['sponsor_type'] == 'sponsor']
introduced_s = sponsored_by_anyone[sponsored_by_anyone['introduced'] == 'Yes']['bioguide_id'].value_counts().mean()
passed_house_s = sponsored_by_anyone[sponsored_by_anyone['passed_house'] == 'Yes']['bioguide_id'].value_counts().mean()
passed_senate_s = sponsored_by_anyone[sponsored_by_anyone['passed_senate'] == 'Yes']['bioguide_id'].value_counts().mean()
to_president_s = sponsored_by_anyone[sponsored_by_anyone['to_president'] == 'Yes']['bioguide_id'].value_counts().mean()
signed_s = sponsored_by_anyone[sponsored_by_anyone['signed'] == 'Yes']['bioguide_id'].value_counts().mean()

cosponsored_by_anyone = bills[bills['sponsor_type'] == 'cosponsor']
introduced_c = cosponsored_by_anyone[cosponsored_by_anyone['introduced'] == 'Yes']['bioguide_id'].value_counts().mean()
passed_house_c = cosponsored_by_anyone[cosponsored_by_anyone['passed_house'] == 'Yes']['bioguide_id'].value_counts().mean()
passed_senate_c = cosponsored_by_anyone[cosponsored_by_anyone['passed_senate'] == 'Yes']['bioguide_id'].value_counts().mean()
to_president_c = cosponsored_by_anyone[cosponsored_by_anyone['to_president'] == 'Yes']['bioguide_id'].value_counts().mean()
signed_c = cosponsored_by_anyone[cosponsored_by_anyone['signed'] == 'Yes']['bioguide_id'].value_counts().mean()

all_topics = list(bills['policy_area'].value_counts().to_dict().keys())

def summary(bioguide_id):

    results = {}

    sponsored = bills[
        (bills['bioguide_id'] == bioguide_id) &
        (bills['sponsor_type'] == 'sponsor')
    ]

    results['sponsored'] = {}
    # sponsored[sponsored['introduced'] == 'Yes'].to_csv('check.csv')
    results['sponsored']['introduced'] = sponsored[sponsored['introduced'] == 'Yes'].shape[0]
    results['sponsored']['passed_house'] = sponsored[sponsored['passed_house'] == 'Yes'].shape[0]
    results['sponsored']['passed_senate'] = sponsored[sponsored['passed_senate'] == 'Yes'].shape[0]
    results['sponsored']['to_president'] = sponsored[sponsored['to_president'] == 'Yes'].shape[0]
    results['sponsored']['signed'] = sponsored[sponsored['signed'] == 'Yes'].shape[0]

    results['sponsored']['introduced-avg'] = int(introduced_s)
    results['sponsored']['passed_house-avg'] = int(passed_house_s)
    results['sponsored']['passed_senate-avg'] = int(passed_senate_s)
    results['sponsored']['to_president-avg'] = int(to_president_s)
    results['sponsored']['signed-avg'] = int(signed_s)

    cosponsored = bills[
        (bills['bioguide_id'] == bioguide_id) &
        (bills['sponsor_type'] == 'cosponsor')
    ]

    results['cosponsored'] = {}
    results['cosponsored']['introduced'] = cosponsored[cosponsored['introduced'] == 'Yes'].shape[0]
    results['cosponsored']['passed_house'] = cosponsored[cosponsored['passed_house'] == 'Yes'].shape[0]
    results['cosponsored']['passed_senate'] = cosponsored[cosponsored['passed_senate'] == 'Yes'].shape[0]
    results['cosponsored']['to_president'] = cosponsored[cosponsored['to_president'] == 'Yes'].shape[0]
    results['cosponsored']['signed'] = cosponsored[cosponsored['signed'] == 'Yes'].shape[0]

    results['cosponsored']['introduced-avg'] = int(introduced_c)
    results['cosponsored']['passed_house-avg'] = int(passed_house_c)
    results['cosponsored']['passed_senate-avg'] = int(passed_senate_c)
    results['cosponsored']['to_president-avg'] = int(to_president_c)
    results['cosponsored']['signed-avg'] = int(signed_c)

    results['topics'] = bills[bills['bioguide_id'] == bioguide_id]['policy_area'].value_counts().to_dict()
    for topic in all_topics:
        if topic not in results['topics']:
            results['topics'][topic] = 0

    return results


# Collect
with dataset.connect(params) as dbx:
    officials = dbx['officials'].find(level = 'national', active = True)

for l in officials:
    bioguide_id = l['bioguide_id']
    results = summary(bioguide_id)
    results['bioguide_id'] = bioguide_id

    with dataset.connect(params) as dbx:
        dbx[tablename].upsert(
            results,
            'bioguide_id'
        )