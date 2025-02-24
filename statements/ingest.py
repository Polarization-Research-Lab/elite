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
dotenv.load_dotenv('../env')
dotenv.load_dotenv(os.environ['PATH_TO_SECRETS'])
api_key = os.environ['CONGRESS_API']

## Connect to DB
db = f"{os.environ['DB_DIALECT']}://{os.environ['DB_USER']}:{urllib.parse.quote(os.environ['DB_PASSWORD'])}@localhost:{os.environ['DB_PORT']}/elite"
logdb = f"{os.environ['DB_DIALECT']}://{os.environ['DB_USER']}:{urllib.parse.quote(os.environ['DB_PASSWORD'])}@localhost:{os.environ['DB_PORT']}/elite"

dbx = dataset.connect(db)
officials = pd.DataFrame(dbx['officials'].find(level = 'national', active = True))
init_count = dbx[ingestor.tablename].count()
dbx.engine.dispose(); dbx.close()

print('starting')
for l_idx, legislator in officials.iterrows():

    # print(legislator['first_name'], legislator['last_name'], l_idx)

    ## Get Date Ranges
    start_date = datetime.date(2024,6,3)

    dbx = dataset.connect(db)
    max_date = sql.select([sql.func.max(dbx[ingestor.tablename].table.c.date)]).where(dbx[ingestor.tablename].table.c.bioguide_id == legislator['bioguide_id']).execute().first()[0]
    dbx.engine.dispose(); dbx.close()

    if max_date: start_date = max_date + datetime.timedelta(days=1)

    end_date = (datetime.datetime.now() - datetime.timedelta(days = 1)).date()

    # Execute Ingester
    # print(f'collecting from {start_date} to {end_date}')
    if start_date < end_date:
        ingestor.ingest(legislator, start_date, end_date, db, logdb)

dbx = dataset.connect(db)
end_count = dbx[ingestor.tablename].count()
dbx.engine.dispose(); dbx.close()

print(f'\titems processed: {end_count - init_count}')




