import sys, json, urllib, datetime, os, time, zlib

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

tweets_media = conn.table('tweets_media')

res = (
    tweets_media
    .filter(_.data.isnull() == False)
    .limit(10)
    .execute()
)

def save(row):
    if row['data']:
        print(row['id'], row['url'])
        with open(f'test2/{row["id"]}.{row["url"].split(".")[-1]}', 'wb') as file:
            file.write(zlib.decompress(row['data']))

res.apply(save, axis = 1)
