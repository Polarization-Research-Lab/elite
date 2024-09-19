import json, io, os
import urllib
import pandas as pd
import numpy as np
import requests
import dataset
import dotenv

def get_twitter_id(twitter_handle):
    api_url = f"https://api.twitter.com/2/users/by/username/{twitter_handle}"
    headers = {"Authorization": f"Bearer {os.getenv('TWITTER_API')}"}
    max_retries = 5  # Maximum number of retries
    backoff_factor = 2  # Factor by which to multiply the wait time with each retry
    wait_time = 1  # Initial wait time in seconds
    
    for attempt in range(max_retries):
        try:
            response = requests.get(api_url, headers=headers)
            response.raise_for_status()  # Raises an HTTPError if the response was an error
            data = response.json()
            return data['data']['id']
        except requests.exceptions.HTTPError as e:
            if response.status_code in [429, 500, 502, 503, 504]:  # Retry-able errors
                print(f"Request failed, retrying in {wait_time} seconds...")
                time.sleep(wait_time)
                wait_time *= backoff_factor  # Increase wait time for next retry
            else:
                print(f"Failed to retrieve Twitter ID due to an error: {e}")
                break  # No retry for client errors or unexpected statuses
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            break
    return None


# Setup

## DB Connection
dotenv.load_dotenv('../env')
dotenv.load_dotenv(os.environ['PATH_TO_SECRETS'])
params = f"{os.environ['DB_DIALECT']}://{os.environ['DB_USER']}:{urllib.parse.quote(os.environ['DB_PASSWORD'])}@localhost:{os.environ['DB_PORT']}/elite"

with dataset.connect(params) as dbx:
    officials = pd.DataFrame(dbx['officials'].find(level = 'national', active = True))

for _, officials in officials.iterrows():

    if officials['twitter_handle'] and officials['twitter_id']:
        pass
    else:
        if officials['twitter_handle']:
            ids = ','.join([
                get_twitter_id(handle.replace('@',''))
                for handle in officials['twitter_handle'].split(',')
            ])

            officials['twitter_id'] = ids

            with dataset.connect(params) as dbx:
                dbx['officials'].update(
                    {
                        'bioguide_id': officials['bioguide_id'],
                        'twitter_id': officials['twitter_id'],
                    }, 
                    ['bioguide_id']
                )
                print(f'updated twitter id for {officials["first_name"]}')