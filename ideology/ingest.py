import subprocess, json, urllib.request, io, os

import dotenv
import pandas as pd
import ibis
from ibis import _

dotenv.load_dotenv('../env')
dotenv.load_dotenv(os.environ['PATH_TO_SECRETS'])

# setup
congress = os.environ['CURRENT_CONGRESS']

## Connect to new elite database with updated users
conn = ibis.mysql.connect(
    host = os.environ['DB_HOST'],
    user = os.environ['DB_USER'],
    password = os.environ['DB_PASSWORD'],
    database = 'elite',
)

legislators = (
    conn.table('officials')
    .filter([_['active'] == 1, _['level'] == 'national'])
    .execute()
)
legislators.to_csv('.tmp/legislators.csv')

# Get Voteview data
url = f"https://voteview.com/static/data/out/members/HS{congress}_members.csv"

# Make the GET request
response = urllib.request.urlopen(url)

# Check the response
if response.getcode() == 200:
    data = pd.read_csv(io.StringIO(response.read().decode('utf8')))

data.to_csv('.tmp/voteview.csv')


# Get Voteview data VOTES
url = f"https://voteview.com/static/data/out/votes/HS{congress}_votes.csv"

# Make the GET request
response = urllib.request.urlopen(url)

# Check the response
if response.getcode() == 200:
    data = pd.read_csv(io.StringIO(response.read().decode('utf8')))

data.to_csv('.tmp/votes.csv')

