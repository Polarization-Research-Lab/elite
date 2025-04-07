# Python Standard Library
import json, urllib, datetime, argparse, os

# External Resources
import dotenv
import dataset
import sqlalchemy as sql
import dataset

# Internal Resources
import ingestor

# Setup
dotenv.load_dotenv('../env')
dotenv.load_dotenv(os.environ['PATH_TO_SECRETS'])
# print(os.environ['IA_ACCESS_KEY'])# = 'YOUR_ACCESS_KEY'
# os.environ['IA_SECRET_KEY']# = 'YOUR_SECRET_KEY'

## Connect to DB
db = f"{os.environ['DB_DIALECT']}://{os.environ['DB_USER']}:{urllib.parse.quote(os.environ['DB_PASSWORD'])}@localhost:{os.environ['DB_PORT']}/elite"

## Get Date Ranges
start_date = datetime.date(2024,6,3)

dbx = dataset.connect(db)
max_date = sql.select(sql.func.max(dbx[ingestor.tablename].table.c.date)).execute().first()[0]
init_count = dbx[ingestor.tablename].count()
dbx.engine.dispose(); dbx.close()

if max_date: start_date = max_date + datetime.timedelta(days=1)

end_date = datetime.datetime.now().date()

# Execute Harvester
for d, day in enumerate(range((end_date - start_date).days)):
    date = start_date + datetime.timedelta(days = day)
    
    print('collecting for:', date)


    ingestor.ingest(date, date, db, None)

    exit()
    dbx = dataset.connect(db)
    existing = dbx[ingestor.tablename].find_one(date = date)
    dbx.engine.dispose(); dbx.close()

    if existing:
        print(f'Skipping {date} since there are already existing entries for that date')

    else:
        ingestor.ingest(date, date, db, None)

dbx = dataset.connect(db)
end_count = dbx[ingestor.tablename].count()
dbx.engine.dispose(); dbx.close()

print(f'\titems processed: {end_count - init_count}')

