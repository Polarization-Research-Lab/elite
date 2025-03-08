import subprocess, json, urllib.request, io, os

import dotenv
import ibis
from ibis import _
import pandas as pd
import dataset
import numpy as np 

# setup
dotenv.load_dotenv('../env')
dotenv.load_dotenv(os.environ['PATH_TO_SECRETS'])

## DB Credentials
params = f"{os.environ['DB_DIALECT']}://{os.environ['DB_USER']}:{urllib.parse.quote(os.environ['DB_PASSWORD'])}@localhost:{os.environ['DB_PORT']}/elite"
conn = ibis.mysql.connect(
    host = os.environ['DB_HOST'],
    user = os.environ['DB_USER'],
    password = os.environ['DB_PASSWORD'],
    database = 'elite',
)

# Link ICPSR and BIOGUIDE
legislators = (
    conn.table('officials')
    .filter([_['active'] == 1, _['level'] == 'national'])
    .execute()
)

voteview_data = pd.read_csv('.tmp/voteview.csv')

ideology = legislators.merge(
    voteview_data[['icpsr','bioguide_id','nominate_dim1']],
    on = 'bioguide_id',
    how = 'left'
)
ideology = ideology.rename(columns = {'nominate_dim1': 'ideology'})
ideology = ideology.replace({np.nan: None})

# Compute the rank (1-based)
ideology['rank'] = ideology['ideology'].rank(method='min', ascending=True)

# Compute the percentile (0 to 1 scale)
ideology['percentile'] = ideology['ideology'].rank(pct=True)

# old version: deprecate
# conservative / liberal ranking and percent
ideology['ideology_rank_lib'] = None
ideology['ideology_rank_con'] = None
ideology['ideology_percentile_lib'] = None
ideology['ideology_percentile_con'] = None
for chamber in legislators['type'].unique():
    for party in legislators['party'].unique():

        if party != 'Independent':

            # conditionals
            ideo = 'lib' if party == 'Democrat' else 'con'
            ascending = True if party == 'Democrat' else False

            # get ideo rank
            ideology.loc[
                (ideology['party'] == party) & \
                (ideology['type'] == chamber)
            , f'ideology_rank_{ideo}'] = ideology.loc[
                (ideology['party'] == party) & \
                (ideology['type'] == chamber)
            , 'ideology'].rank(ascending = ascending, method = 'dense')

            # get ideo percentile
            ideology.loc[
                (ideology['party'] == party) & \
                (ideology['type'] == chamber)
            , f'ideology_percentile_{ideo}'] = ideology.loc[
                (ideology['party'] == party) & \
                (ideology['type'] == chamber)
            , f'ideology_rank_{ideo}'].apply(lambda x: (ideology.loc[
                (ideology['party'] == party) & \
                (ideology['type'] == chamber)
            , f'ideology_rank_{ideo}'] > x).mean())
            # ^ this ones a little confusing, but here's what it's doing: filter for party and chamber, and calculate the number of values that are _greater_ than the value (i.e., X% of people are MORE conservative, or whatever)

ideology = ideology[['bioguide_id', 'ideology', 'ideology_rank_lib', 'ideology_rank_con', 'ideology_percentile_lib', 'ideology_percentile_con', 'rank', 'percentile']]# , 'propublica']] # <-- propublica api is gone :'(
# ideology['ideology_percentile_lib'] = 1 - ideology['ideology_percentile_lib']
# ideology['ideology_percentile_con'] = 1 - ideology['ideology_percentile_con']

ideology['rank_max'] = ideology['rank'].max()

# Save to the Database
ideology = ideology.replace({np.nan: None})
with dataset.connect(params) as dbx:
    dbx['ideology'].upsert_many(
        ideology.to_dict(orient = 'records'),
        'bioguide_id',
    )
