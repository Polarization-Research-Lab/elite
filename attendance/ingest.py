import urllib.request
import pandas as pd 
import io

url = "https://voteview.com/static/data/out/members/HS118_members.csv"

# Make the GET request
response = urllib.request.urlopen(url)

# Check the response
if response.getcode() == 200:
    data = pd.read_csv(io.StringIO(response.read().decode('utf8')))

data.to_csv('.tmp/voteview.csv')

url = "https://voteview.com/static/data/out/votes/HS118_votes.csv"

# Make the GET request
response = urllib.request.urlopen(url)

# Check the response
if response.getcode() == 200:
    data = pd.read_csv(io.StringIO(response.read().decode('utf8')))

data.to_csv('.tmp/votes.csv')

