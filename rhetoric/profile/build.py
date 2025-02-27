﻿
import os, json, urllib, datetime

# External Resources
import dotenv
import dataset
import sqlalchemy as sql
import pandas as pd
import numpy as np
import ibis
from ibis import _

# Setup
categories = ['attack_personal', 'attack_policy', 'outcome_creditclaiming', 'policy', 'outcome_bipartisanship']

## Connect to DB
dotenv.load_dotenv('../../env')
dotenv.load_dotenv(os.environ['PATH_TO_SECRETS'])
params = f"{os.environ['DB_DIALECT']}://{os.environ['DB_USER']}:{urllib.parse.quote(os.environ['DB_PASSWORD'])}@localhost:{os.environ['DB_PORT']}/elite"

conn = ibis.mysql.connect(
    host = os.environ['DB_HOST'],
    user = os.environ['DB_USER'],
    password = os.environ['DB_PASSWORD'],
    database = 'elite',
)
classifications = conn.table('classifications')


# # # # # # # # # # # # 
# # FEDERAL
# # # # # # # # # # # # 
officials = conn.table('officials').filter([_['active'] == 1, _['level'] == 'national'])

# Transform
classifications = classifications

# Aggregate (across all sources)
aggregate_classifications = (
    classifications
    .group_by('bioguide_id')
    .aggregate(
        count = _.count(),
        # **{col + '_count': _[f'{col}'].count() for col in categories},
        **{col + '_sum': _[f'{col}'].sum() for col in categories},
        **{col + '_mean': (_[f'{col}'].mean() * 100).round(2) for col in categories},
    )
    .join(
        officials[['bioguide_id', 'party']],
        _.bioguide_id == officials[['bioguide_id', 'party']].bioguide_id
    )
    .execute()
)

### Get Ranking based on MEAN values ()
for col in categories:
    aggregate_classifications[f"{col}_rank"] = None
    aggregate_classifications.loc[aggregate_classifications['count'] > 300, f"{col}_rank"] = aggregate_classifications.loc[aggregate_classifications['count'] > 300, f"{col}_mean"].rank(ascending=False, method = 'dense') 
    aggregate_classifications = aggregate_classifications.astype({col: 'int' for col in aggregate_classifications.select_dtypes(include=['int64']).columns}) # <-- convert to basic int

aggregate_classifications['source'] = 'all'

# # Aggregate (by source)
aggregate_by_source = (
    classifications
    .group_by(['bioguide_id', 'source'])
    .aggregate(
        count = classifications['bioguide_id'].count(),
        **{col + '_count': classifications[f'{col}'].count() for col in categories},
        **{col + '_sum': classifications[f'{col}'].sum() for col in categories},
        **{col + '_mean': (classifications[f'{col}'].mean() * 100).round(2) for col in categories},
    )
    .execute()
)

agg = pd.concat(
    [aggregate_classifications, aggregate_by_source],
)

agg = agg.replace(np.nan, None)

with dataset.connect(params) as dbx:
    dbx['rhetoric'].delete()
    dbx['rhetoric'].insert_many(
        agg.to_dict(orient = 'records'),
    )

# # # # # # # # # # # 
# STATE
# # # # # # # # # # # 
officials = conn.table('officials').filter([_['active'] == 1, _['level'] == 'state'])

# Transform
classifications = classifications

# Aggregate (across all sources)
aggregate_classifications = (
    classifications
    .group_by('openstates_id')
    .aggregate(
        count = _.count(),
        **{col + '_count': _[f'{col}'].count() for col in categories},
        **{col + '_sum': _[f'{col}'].sum() for col in categories},
        **{col + '_mean': (_[f'{col}'].mean() * 100).round(2) for col in categories},
    )
    .join(
        officials[['openstates_id', 'party', 'state']],
        _.openstates_id == officials[['openstates_id', 'party', 'state']].openstates_id
    )
    .execute()
)

### Get Ranking based on MEAN values ()
for col in categories:
    aggregate_classifications[f"{col}_rank"] = None

    # International Rankings:
    # aggregate_classifications.loc[aggregate_classifications['count'] > 300, f"{col}_rank"] = aggregate_classifications.loc[aggregate_classifications['count'] > 300, f"{col}_mean"].rank(ascending=False, method = 'dense') 

    # Ranking by State:
    aggregate_classifications.loc[aggregate_classifications['count'] > 300, f"{col}_rank"] = (
        aggregate_classifications.loc[aggregate_classifications['count'] > 300]
        .groupby("state")[f"{col}_mean"]
        .rank(ascending = False, method = "dense")
    )

    aggregate_classifications = aggregate_classifications.astype({col: 'int' for col in aggregate_classifications.select_dtypes(include=['int64']).columns}) # <-- convert to basic int


aggregate_classifications['source'] = 'all'

# # Aggregate (by source)
aggregate_by_source = (
    classifications
    .group_by(['openstates_id', 'source'])
    .aggregate(
        count = classifications['openstates_id'].count(),
        **{col + '_count': classifications[f'{col}'].count() for col in categories},
        **{col + '_sum': classifications[f'{col}'].sum() for col in categories},
        **{col + '_mean': (classifications[f'{col}'].mean() * 100).round(2) for col in categories},
    )
    .execute()
)

agg = pd.concat(
    [aggregate_classifications, aggregate_by_source],
)

agg = agg.replace(np.nan, None)

with dataset.connect(params) as dbx:
    dbx['rhetoric_state'].delete()
    dbx['rhetoric_state'].insert_many(
        agg.to_dict(orient = 'records'),
    )
