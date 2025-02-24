import sys, json, urllib, datetime, os, time

import dotenv
import requests
import ibis
from ibis import _
import dataset


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

# get all not in tweets_media
res = (
    tweets
    .filter(tweets.media['media_keys'].notnull())
    .left_join(
        tweets_media, _.id == tweets_media.tweet_table_id
    )
    .filter(tweets_media.tweet_table_id.isnull())
    .select(_.id.name('tweet_table_id'), _['media'], _['tweet_id'])
    .execute()
)

res['media'] = res['media'].apply(lambda x: x['media_keys'])

# Explode the 'media_keys' list into individual rows
res = res.explode('media')
res = res.rename(columns = {'media': 'media_key'})
res = res[['media_key', 'tweet_id', 'tweet_table_id']]

chunk_size = 500
for start in range(0, len(res), chunk_size):
    print(f'chunk: {start}')

    chunk = res.iloc[start:start + chunk_size]
    with dataset.connect(params) as dbx:
        dbx['tweets_media'].insert_many(
            chunk.to_dict(orient='records')
        )



