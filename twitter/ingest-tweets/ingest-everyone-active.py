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
logdb = f"{os.environ['DB_DIALECT']}://{os.environ['DB_USER']}:{urllib.parse.quote(os.environ['DB_PASSWORD'])}@localhost:{os.environ['DB_PORT']}/elite"


dbx = dataset.connect(db)
officials = pd.DataFrame(dbx['officials'].find(active = True))
print('start counts | federal:', dbx['tweets'].count(), 'state:', dbx['tweets_state'].count())
dbx.engine.dispose(); dbx.close()

for l_idx, legislator in officials.iloc[:100].iterrows():

    # print(legislator['first_name'], legislator['last_name'], l_idx, legislator['level'])

    if legislator['level'] == 'national': 
        tablename = 'tweets'
        id_col = 'bioguide_id'
    elif legislator['level'] == 'state': 
        tablename = 'tweets_state'
        id_col = 'openstates_id'
    else: 
        print('MISSING LEVEL!')
        exit()

    ## Get Date Ranges
    start_date = datetime.date(2024,1,1)

    dbx = dataset.connect(db)
    max_date = sql.select([sql.func.max(dbx[tablename].table.c.date)]).where(dbx[tablename].table.c[id_col] == legislator[id_col]).execute().first()[0]
    dbx.engine.dispose(); dbx.close()

    if max_date: start_date = max_date + datetime.timedelta(days=1)

    # end_date = (datetime.datetime.now() - datetime.timedelta(days = 1)).date()
    end_date = datetime.datetime.now().date()

    # Execute Harvester
    if start_date < end_date:
        ingestor.ingest(legislator, start_date, end_date, db, logdb, api_key)


dbx = dataset.connect(db)
print('end counts | federal:', dbx['tweets'].count(), 'state:', dbx['tweets_state'].count())
dbx.engine.dispose(); dbx.close()
