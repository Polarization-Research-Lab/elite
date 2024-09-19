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

# Internal Dependencies
import llms
from prompts import attack, outcomes, policy, all_category

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

if __name__ == '__main__':
    print('---starting---')
    count = classifications.count().execute()

    total_processed = {
        # 'attack': 0,
        # 'policy': 0,
        # 'outcomes': 0,
        'all_category': 0,
    }

    today = datetime.date.today()
    # today = datetime.date(year=2024,month=5,day=25)

    beginning_date = datetime.date(year=2024, month=5, day=21)
    # beginning_date = datetime.date(year=2024, month=5, day=15)
    # beginning_date = datetime.date(year=2023, month=12, day=20)
    # beginning_date = datetime.date(year=2024, month=1, day=1)
    # beginning_date = datetime.date(year=2022,month=8,day=1) # <-- go back to the beginning

    for d, day in enumerate(range((today - beginning_date).days + 1)):
        target_date = today - datetime.timedelta(days = day) # <-- start from lastest date (go backward)
       
        print(target_date)

        chunk = (
            classifications 
            .filter(classifications.date == target_date)
        )
        for prompt in prompts:

            combined_filter = ibis.literal(False)
            for col in prompts[prompt].column_map.values(): # Chain OR conditions for each column
                combined_filter = combined_filter | chunk[col['name']].isnull()

            num_items_that_need_classifying = (
                chunk 
                .filter(combined_filter)
                .count()
                .execute()
            )

            print('\t', prompt, '|', num_items_that_need_classifying)

            if num_items_that_need_classifying > 0:
                total_processed[prompt] += num_items_that_need_classifying

                items_that_need_classifying = (
                    chunk
                    .select(['id','source','source_id','bioguide_id','text', 'errors'] + [col['name'] for col in prompts[prompt].column_map.values()])
                    .filter(combined_filter)
                    .execute()
                )

                items_that_need_classifying['errors'] = items_that_need_classifying['errors'].apply(lambda x: {} if x is None else x)
                items_that_need_classifying['response'] = None
                items_that_need_classifying['message'] = items_that_need_classifying['text'].apply(lambda text: prompts[prompt].prompt.format(target = text))

                batches = llms.send_batch(items_that_need_classifying, prompt, model = 'gpt-4o')
                print(batches)

                for batch in batches:
                    with database.connect(log_params) as dbx:
                        dbx['openai_batch_api'].insert(
                            {
                                'batch_id': batch,
                                'status': 'sent',
                            }
                        )

                print(f'sent {batches}')

