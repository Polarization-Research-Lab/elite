'''
Actually classify the data
'''
# Python Standard Library
import sys, json, urllib, datetime, os, time
import concurrent.futures

# External Dependencies
import dotenv
import numpy as np 
import pandas as pd
import dask.dataframe as dd  

import sqlalchemy as sql
import dataset
import ibis
from ibis import _

# import json5
import hjson

# Internal Dependencies
import llms
import prompt

dotenv.load_dotenv('../../../env')
dotenv.load_dotenv(os.environ['PATH_TO_SECRETS'])

## DB Credentials
params = f"{os.environ['DB_DIALECT']}://{os.environ['DB_USER']}:{urllib.parse.quote(os.environ['DB_PASSWORD'])}@localhost:{os.environ['DB_PORT']}/elite"
conn = ibis.mysql.connect(
    host = os.environ['DB_HOST'],
    user = os.environ['DB_USER'],
    password = os.environ['DB_PASSWORD'],
    database = 'elite',
)
unclassified_items = (
    conn.table('classifications')
    .select([
        'id',
        'text',
        'date',
        'classified',
        'attack_personal',
        'attack_type',
        'attack_target',
        'attack_policy',
        'outcome_bipartisanship',
        'outcome_creditclaiming',
        'policy',
        'policy_area',
        'extreme_label',
    ])
    .filter([
        _.date >= '2023-01-01', # <-- we dont care about items before then
        (_.classified != 1) | _.classified.isnull()
    ])
)

count = unclassified_items.count().execute()
chunksize = 1000
num_chunks = int(count / chunksize) + 1

print(f'''
RUNNING COLLECTION
count: {count}
chunksize: {chunksize}
num_chunks: {num_chunks}
''')

# for c in range(num_chunks):
for c in range(num_chunks):
    print(f'\tCollecting chunk {c}')

    chunk = (
        unclassified_items
        .limit(chunksize)
        .execute()
    )

    if chunk.shape[0] == 0:
        print('\tNOTHING TO CLASSIFY (SOMETHING PROBABLY WENT WRONG FOR THIS MESSAGE TO SHOW); BREAKING')
        break

    chunk = dd.from_pandas(chunk, npartitions = 16)
    chunk = (
       chunk 
        .apply(
            prompt.pipeline, 
            axis = 1,
            meta={
                'id': 'int64', 
                'text': 'object', 
                'date': 'datetime64[ns]', 
                'classified': 'int64',
                'attack_personal': 'int64', 
                'attack_type': 'object',
                'attack_target': 'object',
                'attack_policy': 'int64',
                'outcome_bipartisanship': 'int64',
                'outcome_creditclaiming': 'int64',
                'policy': 'int64',
                'policy_area': 'object',
                'extreme_label': 'object',
            }
        )
        .compute()
    )

    print('\t ** classification successful **')

    chunk = chunk.replace({np.nan: None})
    
    dbx = dataset.connect(params)
    dbx['classifications'].upsert_many(
        chunk.to_dict(orient = 'records'),
        'id'
    )
    dbx.engine.dispose(); dbx.close()

    print('\t ** upsert successful **')
    print(f'\n----------------classified: {chunk.shape[0]} items--------------\n\n')

print('==== DONE ====')
