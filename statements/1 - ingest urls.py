import subprocess, json, urllib.request, sys, io, os, json, datetime, time

import dotenv
import ibis; from ibis import _
import pandas as pd
import dataset
import numpy as np 

import ingestion_utils

# setup
dotenv.load_dotenv('../env')
dotenv.load_dotenv(os.environ['PATH_TO_SECRETS'])

## Logging
log = ingestion_utils.DualLogger()

## DB Credentials
db_uri = f"{os.environ['DB_DIALECT']}://{os.environ['DB_USER']}:{urllib.parse.quote(os.environ['DB_PASSWORD'])}@localhost:{os.environ['DB_PORT']}/elite"
conn = ibis.mysql.connect(host = os.environ['DB_HOST'], user = os.environ['DB_USER'], password = os.environ['DB_PASSWORD'], database = 'elite')

officials = conn.table('officials').filter([_.level == 'national', _.active == 1])
statements_scrape_params = conn.table('statements_scrape_params')

# Join officials and statements_scrape_params
officials_w_params = (
    officials
    .join(
        statements_scrape_params,
        officials.bioguide_id == statements_scrape_params.bioguide_id
    )
    .execute()
    .replace({np.nan: None})
)

conn.disconnect()

# # # # # # # #
# RUN
# # # # # # # #
for o, official in officials_w_params.iterrows():
# for o, official in officials_w_params.iloc[3:10].iterrows():
   
    log.start() # <-- captures stdout (but also prints to screen)
    
    # Run Ingestor
    # - - - - - - - - - - - - - - - - - - - - -
    try:
        urls, error, error_text = ingestion_utils.ingest_new_urls_from_press_page(official, db_uri)    
    except Exception as e:
        print(f'\tEntire pipeline failed with: {e}')
        urls = None
        error = 1
        error_text = e

    # Clean Up And Logging
    # - - - - - - - - - - - - - - - - - - - - -
    print('\tURL Collection Finished. Cleaning up...')


    print('\t\t\tSaving logging info to params table')
    output = log.finish() # <-- collects stdout
    dbx = dataset.connect(db_uri)
    dbx['statements_scrape_params'].update(
        {
            'bioguide_id': official.bioguide_id,
            'last_run_error': error,
            'last_run_error_text': error_text,
            'last_run_output': output,
        },
        'bioguide_id'
    )
    dbx.engine.dispose(); dbx.close()

    if error == 1:
        print('\t\t\tError Detected. Saving output and will NOT push urls to statements table.')
    else:
        print(f'\t\t\tNo Errors Detected. Assuming everything went correctly and pushing {urls.shape[0]} urls to database.')
        dbx = dataset.connect(db_uri)
        dbx['statements'].upsert_many(
            urls.to_dict(orient = 'records'),
            'url'
        )
        dbx.engine.dispose(); dbx.close()
    print('=== Scrape Complete ===')

