import json, io, os
import urllib
import pandas as pd
import numpy as np
import requests
import dataset

os.makedirs('./.tmp/', exist_ok = True)

# Load database configuration from a JSON file
url = "https://theunitedstates.io/congress-legislators/legislators-current.csv"
with open('.tmp/legislators-current.csv', 'wb') as file:
    file.write(requests.get(url).content)

url = "https://theunitedstates.io/congress-legislators/legislators-current.json"
with open('.tmp/legislators-current.json', 'wb') as file:
    file.write(requests.get(url).content)

url = "https://theunitedstates.io/congress-legislators/legislators-social-media.json"
data = json.loads(requests.get(url).content)
data = {
    item['id']['bioguide']: item['social']
    for item in data
}
with open('.tmp/legislators-social.json', 'w') as file:
    json.dump(data, file)
