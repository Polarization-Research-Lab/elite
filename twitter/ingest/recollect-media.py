# Python Standard Library
import json, urllib, datetime, argparse, os

# External Resources
import dotenv
import dataset
import sqlalchemy as sql
import dataset
import pandas as pd 

# Internal Resources
import ingestor

# Setup
dotenv.load_dotenv('../../env')
dotenv.load_dotenv(os.environ['PATH_TO_SECRETS'])
api_key = os.environ['TWITTER_API']

## Connect to DB
db = f"{os.environ['DB_DIALECT']}://{os.environ['DB_USER']}:{urllib.parse.quote(os.environ['DB_PASSWORD'])}@localhost:{os.environ['DB_PORT']}/elite"
# logdb = f"{os.environ['DB_DIALECT']}://{os.environ['DB_USER']}:{urllib.parse.quote(os.environ['DB_PASSWORD'])}@localhost:{os.environ['DB_PORT']}/elite"

dbx = dataset.connect(db)
officials = pd.DataFrame(dbx['officials'].find(active = True))
init_count = dbx[ingestor.tablename].count()
dbx.engine.dispose(); dbx.close()

for l_idx, legislator in officials.iterrows():
    print(legislator['first_name'], legislator['last_name'], l_idx)
    dbx = dataset.connect(db) 
    tweets = pd.DataFrame(
        dbx.query(
            f"""
            SELECT *
            FROM tweets
            WHERE bioguide_id = '{legislator['bioguide_id']}'
              AND (public_metrics IS NULL OR JSON_LENGTH(public_metrics) = 0)
              AND tweet_id IS NOT NULL
            """
              # AND original_not_found != 1
        )
    )
    dbx.engine.dispose(); dbx.close()

    updates = []
    for t_idx, tweet in tweets.iterrows():

        resp = (
            ingestor
            .get_tweets_by_tweet_id(tweet['tweet_id'], api_key)
        )

        if resp:
            if resp.get('errors'):
                updates.append({
                    'tweet_id': tweet['tweet_id'],
                    'original_not_found': 1,
                })
                print(f"nothing found for {legislator['name']} | tweet idx: {tweet['id']} | tweet_id: {tweet['tweet_id']}")
            else:
                public_metrics = (
                    resp
                    .get('data', {})
                    .get('public_metrics')
                )

                if public_metrics:
                    updates.append({
                        'tweet_id': tweet['tweet_id'],
                        'public_metrics': public_metrics,
                        'original_not_found': 0,
                    })

                else:
                    print(f"failed with {legislator['name']} | tweet idx: {tweet['id']} | tweet_id: {tweet['tweet_id']}")

    print(len(updates), '<-- len updates')
    dbx = dataset.connect(db)
    dbx['tweets'].upsert_many(
        updates,
        'tweet_id'
    )
    dbx.engine.dispose(); dbx.close()
    print('upserted ')

# 192821 <-- count from `select count(*) from tweets where public_metrics is null`
