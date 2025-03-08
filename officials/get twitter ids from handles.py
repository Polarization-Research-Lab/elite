import json, io, os, urllib, re, time
import pandas as pd
import numpy as np
import requests
import dataset
import dotenv

# Setup
def get_twitter_id(twitter_handle):
    api_url = f"https://api.twitter.com/2/users/by/username/{twitter_handle}"
    headers = {"Authorization": f"Bearer {os.getenv('TWITTER_API')}"}
    max_retries = 5  # Maximum number of retries
    backoff_factor = 2  # Factor by which to multiply the wait time with each retry
    wait_time = 15 * 60  # Initial wait time in seconds
    
    for attempt in range(max_retries):
        try:
            response = requests.get(api_url, headers=headers)
            response.raise_for_status()  # Raises an HTTPError if the response was an error
            data = response.json()
            if data.get('data'):
                return data['data']['id']
            else:
                print(f'DATA NOT FOUND IN {data}')
        except requests.exceptions.HTTPError as e:
            if response.status_code in [429, 500, 502, 503, 504]:  # Retry-able errors
                print(f"Request failed, retrying in {wait_time} seconds. Error was {response}\n")
                time.sleep(wait_time)
                wait_time = 15 * 60 # Increase wait time for next retry
            else:
                print(f"Failed to retrieve Twitter ID due to an error: {e}")
                break  # No retry for client errors or unexpected statuses
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            break
    return None

def clean_twitter_handle(handle):
    # Replace the Twitter URL prefix
    handle = (
        handle
        .replace('https://twitter.com/', '')
        .replace('https://www.twitter.com/', '')
        .replace('https://mobile.twitter.com/', '')
        .replace('https://www.mobile.twitter.com/', '')
        .replace('https://x.com/', '')
        .replace('https://www.x.com/', '')
        .replace('twitter.com/', '')
        .replace('www.twitter.com/', '')
        .replace('mobile.twitter.com/', '')
        .replace('www.mobile.twitter.com/', '')
        .replace('x.com/', '')
        .replace('www.x.com/', '')
        .replace('@', '')
        .replace('?lang=en', '')
        .strip()
    )
    # Remove any query parameters like '?lang=en'
    handle = handle.split('?')[0].strip()
    return handle

## DB Connection
dotenv.load_dotenv('../env')
dotenv.load_dotenv(os.environ['PATH_TO_SECRETS'])
params = f"{os.environ['DB_DIALECT']}://{os.environ['DB_USER']}:{urllib.parse.quote(os.environ['DB_PASSWORD'])}@localhost:{os.environ['DB_PORT']}/elite"


# Execution
dbx = dataset.connect(params)
officials = dbx['officials'].find(active = True)
dbx.engine.dispose(); dbx.close()

for official in officials:

    # do they have a twitter_id? if not, find it from the handle
    if not official['twitter_id']:

        if official['twitter_handle']: # <-- are there any handles to look for? if so: pull the id

            ids = []
            errs = ''
            official['twitter_handle'] = official['twitter_handle'].replace(' ',',')
            for handle in official['twitter_handle'].split(','):
                cleaned = clean_twitter_handle(handle)
                if cleaned != '':
                    id_returned = get_twitter_id(cleaned)
                    if id_returned:
                        ids.append(id_returned.replace('@',''))
                    else:
                        print(f'\tfailed to pull for {official["name"]} w/ {handle}')
                        errs += f'likely incorrect handle: {official["name"]} w/ {handle}\n'
                else:
                    print(f'returned blank for {handle}')
                    errs += f'likely incorrect handle: {official["name"]} w/ {handle}\n'

            if len(ids) > 0:
                ids = ','.join(ids)
            else:
                ids = None

            if errs != '': 
                if not official.get('error_flags'):
                    official['error_flags'] = {'twitter_handle': errs}
                else:                    
                    official['error_flags']['twitter_handle'] = errs

            if ids:
                official['twitter_id'] = ids

                dbx = dataset.connect(params)
                dbx['officials'].update(
                    {
                        'id': official['id'],
                        'twitter_id': ids,
                        'error_flags': official['error_flags'],
                    }, 
                    ['id']
                )
                dbx.engine.dispose(); dbx.close()
                print(f'updated twitter id for {official["name"]} | {official["twitter_id"]}')
        
            elif errs != '':            
                dbx = dataset.connect(params)
                dbx['officials'].update(
                    {
                        'id': official['id'],
                        'error_flags': official['error_flags'],
                    }, 
                    ['id']
                )
                dbx.engine.dispose(); dbx.close()
                print(f'!!!!!!!!!! FAILED FOR {official["name"]} | {official["twitter_id"]} w/ error {errs}')
            else:
                print(f'!!!!!!!!!! FAILED FOR {official["name"]} | {official["twitter_id"]} w/ no errs !!!!!!!!!!!!!')

    # time.sleep(.5)
