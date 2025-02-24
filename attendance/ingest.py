import urllib.request, os

import pandas as pd 
import io
import dotenv

dotenv.load_dotenv('../env')

url = f"https://voteview.com/static/data/out/members/HS{os.environ['CURRENT_CONGRESS']}_members.csv"

# Make the GET request
response = urllib.request.urlopen(url)

# Check the response
if response.getcode() == 200:
    data = pd.read_csv(io.StringIO(response.read().decode('utf8')))

data.to_csv('.tmp/voteview.csv')

url = f"https://voteview.com/static/data/out/votes/HS{os.environ['CURRENT_CONGRESS']}_votes.csv"

# Make the GET request
response = urllib.request.urlopen(url)

# Check the response
if response.getcode() == 200:
    data = pd.read_csv(io.StringIO(response.read().decode('utf8')))

data.to_csv('.tmp/votes.csv')

