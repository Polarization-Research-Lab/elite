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

import sqlalchemy as sql
import dataset as database
import ibis
from ibis import _
import openai
import hjson

# Internal Dependencies
import llms
from prompts import attack, outcomes, policy, all_category
from openai import OpenAI

# Setup
prompts = {
    # 'attack': attack,
    # 'outcomes': outcomes,
    # 'policy': policy,
    'all_category': all_category,
}

dotenv.load_dotenv('../../env')
dotenv.load_dotenv(os.environ['PATH_TO_SECRETS'])

## DB Credentials
params = f"{os.environ['DB_DIALECT']}://{os.environ['DB_USER']}:{urllib.parse.quote(os.environ['DB_PASSWORD'])}@localhost:{os.environ['DB_PORT']}/elite"
conn = ibis.mysql.connect(
    host = os.environ['DB_HOST'],
    user = os.environ['DB_USER'],
    password = os.environ['DB_PASSWORD'],
    database = 'elite',
)
classifications = conn.table('classifications')
log_params = f"{os.environ['DB_DIALECT']}://{os.environ['DB_USER']}:{urllib.parse.quote(os.environ['DB_PASSWORD'])}@localhost:{os.environ['DB_PORT']}/log"

debug = False

## Process Reponse Function
def process_response(item, prompt):
    try:
        if '{' in item['response']:
            item['response'] = item['response'][item['response'].find('{'):item['response'].rfind('}')+1]
        else: 
            raise ValueError('Bad JSON detected')

        item['response'] = hjson.loads(item['response'].replace("“",'"').replace("”", '"'))
        for key in prompts[prompt].column_map:
            col = prompts[prompt].column_map[key]['name'] # <-- what we actually name the column
            item[col] = prompts[prompt].column_map[key]['filter'](
                item['response'][key]
            )
        return item

    except Exception as exception:
        item['errors'][prompt] = f"Error in processing {item['source']}-{item['source_id']}.\n===\nResponse: {item['response']}\n===\nException: {exception}"
        # raise(exception)
        return item



with database.connect(log_params) as dbx:
    batches = pd.DataFrame(dbx['openai_batch_api'].find(status = 'sent'))
print(batches)



for b, batch_row in batches.iterrows():

    with openai.OpenAI() as client:
        batch = client.batches.retrieve(batch_row['batch_id'])

    if batch.status == 'completed':
        print(f'processing batch {batch.id}')
        with openai.OpenAI() as client:
            try:
                content = client.files.content(batch.output_file_id).read()
            except:
                print('no file found or file was deleted')
                content = None 

        if content:
            print('\nitems found; processing')
            items = [json.loads(line) for line in content.splitlines()]

            prompt = items[0]['custom_id'].split('-')[0] # <-- we can count on all items coming from the same prompt
            prompt = 'all_category'

            items = pd.DataFrame(
                [
                    {
                        'id': item['custom_id'],
                        'response': item['response']['body']['choices'][0]['message']['content'],
                    }
                    for item in items
                ]
            )

            items['id'] = items['id'].apply(lambda x: int(x.split('-')[-1]))

            database_items = (
                classifications
                .filter(_['id'].isin(items['id'].tolist()))
                .execute()
            )

            items = pd.merge(items, database_items, on = 'id')

            # format results
            items = items.apply(lambda item: process_response(item, prompt), axis = 1)

            new_cols = [col['name'] for col in prompts[prompt].column_map.values()]

            results = items[['id'] + new_cols]
            results = results.fillna(np.nan).replace([np.nan], [None])

            # upload results
            if debug == False:
                with database.connect(params) as dbx:
                    dbx['classifications'].upsert_many(
                        results.to_dict(orient = 'records'),
                        'id'
                    )
            print('\tupserted')

            with database.connect(log_params) as dbx:
                batch_row['status'] = 'complete'
                dbx['openai_batch_api'].update(
                    batch_row,
                    'id'
                )
            print('\tupdated log')

        print('---done---')
