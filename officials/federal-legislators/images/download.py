'''
Download Images
'''
import os, json, urllib.request, time

import ibis
import pandas as pd
import requests
import dotenv

dotenv.load_dotenv('../../../env')
dotenv.load_dotenv(os.environ['PATH_TO_SECRETS'])
db = ibis.mysql.connect(
    host = os.environ['DB_HOST'],
    user = os.environ['DB_USER'],
    password = os.environ['DB_PASSWORD'],
    database = 'elite',
)

legislators = db.table('legislators')
legislators = (
    legislators
    .filter(legislators['propublica']['in_office'].cast('int64') == 1)
    .execute()
)

for bioguide_id in legislators['bioguide_id']:
    print(bioguide_id)
    url = f"https://theunitedstates.io/images/congress/450x550/{bioguide_id}.jpg"
    response = requests.get(url)
    time.sleep(.5)  # Delay to avoid rate limiting

    if response.headers['Content-Type'].startswith('image') and response.status_code == 200:
        # Check if the response contains image data and that it's not empty
        if len(response.content) > 0:
            file_path = f'set/{bioguide_id}.jpg'
            with open(file_path, 'wb') as file:
                file.write(response.content)
        else:
            print('NONE!!@!!!')
            print(f"No image data received for {bioguide_id}, skipping...")