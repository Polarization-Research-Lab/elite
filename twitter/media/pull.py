import sys, json, urllib, datetime, os, time, zlib

import dotenv
import requests
import ibis
from ibis import _
import backoff
import dataset
import pandas as pd 

def backoff_handler(details):
    print(f"Backing off {details['wait']} seconds after {details['tries']} tries. Exception: {details['exception']}")

dotenv.load_dotenv('../../env')
dotenv.load_dotenv(os.environ['PATH_TO_SECRETS'])
params = f"{os.environ['DB_DIALECT']}://{os.environ['DB_USER']}:{urllib.parse.quote(os.environ['DB_PASSWORD'])}@localhost:{os.environ['DB_PORT']}/elite"
conn = ibis.mysql.connect(
    host = os.environ['DB_HOST'],
    user = os.environ['DB_USER'],
    password = os.environ['DB_PASSWORD'],
    database = 'elite',
)

tweets = conn.table('tweets')
tweets_media = conn.table('tweets_media')


# Function to get media data from Twitter API using the media key
@backoff.on_exception(backoff.expo, Exception, max_tries=8, on_backoff=backoff_handler, factor=5)
def get_media(tweet_id):
    url = f"https://api.twitter.com/2/tweets/{tweet_id}?expansions=attachments.media_keys&media.fields=url"
    headers = {"Authorization": f"Bearer {os.environ['TWITTER_API']}"}

    response = requests.get(url, headers=headers)
    response.raise_for_status()  # Raises HTTPError for bad requests
 
    data = response.json()
    
    images = {}
    for media in data.get('includes', {}).get('media', []):
        if media.get('type') == 'photo':
            images[media['media_key']]  = media.get('url')
    return images

@backoff.on_exception(backoff.expo, Exception, max_tries=8, on_backoff=backoff_handler, factor=5)
def process(row):
    images = get_media(row['tweet_id'])
    if images.get(row['media_key']): 
        response = requests.get(images.get(row['media_key']))
        response.raise_for_status()  # Raises HTTPError for bad requests

        if response.status_code == 200:
            return pd.Series([zlib.compress(response.content), images.get(row['media_key'])])
        else:
            print(f"ERROR: {response.text}")
            return pd.Series([None, images.get(row['media_key'])])
    return pd.Series([None, row['url']])


# get all not in tweets_media
joined = (
    tweets_media
    .filter(_.data.isnull())
    # .filter(_.url.isnull())
    .left_join(
        tweets, _.tweet_table_id == tweets.id
    )
    .select([_.id, _.data, _.media_key, _.tweet_id, _.url])
    .execute()
)

chunk_size = 50
for start in range(0, len(joined), chunk_size):
    print(f'chunk: {start}')

    chunk = joined.iloc[start:start + chunk_size]
    chunk[['data', 'url']] = chunk.apply(process, axis = 1, result_type='expand')
    with dataset.connect(params) as dbx:
        dbx['tweets_media'].upsert_many(
            chunk.to_dict(orient = 'records'),
            'id'
        )

