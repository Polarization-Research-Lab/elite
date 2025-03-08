# from scrapegraph.py import Client

# client = Client(api_key="sgai-xxxxxx")

# response = client.smartscraper(
#     website_url="https://aderholt.house.gov/",
#     user_prompt="Whats the title on the home page?"
# )


import subprocess, json, urllib.request, io, os, json

import dotenv
import ibis
import pandas as pd
import dataset
import numpy as np 

from scrapegraphai.graphs import SmartScraperGraph

# setup
dotenv.load_dotenv('../env')
dotenv.load_dotenv(os.environ['PATH_TO_SECRETS'])

## DB Credentials
params = f"{os.environ['DB_DIALECT']}://{os.environ['DB_USER']}:{urllib.parse.quote(os.environ['DB_PASSWORD'])}@localhost:{os.environ['DB_PORT']}/elite"
conn = ibis.mysql.connect(
    host = os.environ['DB_HOST'],
    user = os.environ['DB_USER'],
    password = os.environ['DB_PASSWORD'],
    database = 'elite',
)

# Define the configuration for the scraping pipeline
graph_config = {
   "llm": {
       "api_key": os.environ['OPENAI_API_KEY'],
       "model": "openai/gpt-4o-mini",
   },
   "verbose": True,
   "headless": True,
}

# Create the SmartScraperGraph instance
smart_scraper_graph = SmartScraperGraph(
    source="https://aderholt.house.gov/media-center/press-releases",
    # prompt="From this webpage, return the url to press releases or personal statements. If you can't find it, return null in the json.",
    prompt="From this webpage, how can i get to the next page of press release results?",
    # prompt="From this webpage, return all ",    k
    config=graph_config
)

# Run the pipeline
result = smart_scraper_graph.run()

print(json.dumps(result, indent=4))

# https://aderholt.house.gov/media-center/press-releases/congressman-robert-aderholt-applauds-president-trumps-address-congress
# https://aderholt.house.gov/media-center/press-releases/congressman-robert-aderholt-applauds-president-trumps-address-congress