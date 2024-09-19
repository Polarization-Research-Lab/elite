import os, json, urllib, pickle, time

import dotenv
import pandas as pd
import dataset

dotenv.load_dotenv('../env')
dotenv.load_dotenv(os.environ['PATH_TO_SECRETS'])
params = f"{os.environ['DB_DIALECT']}://{os.environ['DB_USER']}:{urllib.parse.quote(os.environ['DB_PASSWORD'])}@localhost:{os.environ['DB_PORT']}/elite"

# DB Table Build
tablename = 'attendance'
with dataset.connect(params) as dbx:
    table = dbx.create_table(tablename, primary_id = 'id', primary_type = dbx.types.integer, primary_increment = True)
    table.create_column('bioguide_id', dbx.types.string(7), unique = True, nullable = False)

# get voteview data
voteview = pd.read_csv('.tmp/voteview.csv')

congress = 118

max_sen_votes = int(voteview[(voteview['chamber'] == 'Senate')]['nominate_number_of_votes'].max())
avg_sen_votes = int(voteview[(voteview['chamber'] == 'Senate')]['nominate_number_of_votes'].mean())

max_rep_votes = int(voteview[(voteview['chamber'] == 'House')]['nominate_number_of_votes'].max())
avg_rep_votes = int(voteview[(voteview['chamber'] == 'House')]['nominate_number_of_votes'].mean())

def summary(bioguide_id):
    chamber = voteview[(voteview['bioguide_id'] == bioguide_id)]['chamber'].iloc[0]
    results = {
        'total': int(voteview[(voteview['bioguide_id'] == bioguide_id)]['nominate_number_of_votes'].iloc[0]),
        'max': max_rep_votes if chamber == 'House' else max_sen_votes,
        'avg': avg_rep_votes if chamber == 'House' else avg_sen_votes,
    }
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


