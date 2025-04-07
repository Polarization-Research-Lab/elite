import subprocess, json, urllib.request, sys, io, os, json, datetime, time, random

import dotenv
import ibis; from ibis import _
import pandas as pd
import dataset
import numpy as np 
import trafilatura  # Modern web scraping library with built-in handling
from tenacity import retry, stop_after_attempt, wait_exponential

import util

# setup
dotenv.load_dotenv('../env')
dotenv.load_dotenv(os.environ['PATH_TO_SECRETS'])

## DB Credentials
db_uri = f"{os.environ['DB_DIALECT']}://{os.environ['DB_USER']}:{urllib.parse.quote(os.environ['DB_PASSWORD'])}@localhost:{os.environ['DB_PORT']}/elite"
conn = ibis.mysql.connect(
    host = os.environ['DB_HOST'],
    user = os.environ['DB_USER'],
    password = os.environ['DB_PASSWORD'],
    database = 'elite',
)
statements = conn.table('statements')

## Fetch unscraped press releases (content_has_been_scraped == 0 or NULL)
unscraped_press_releases = (
    statements
    .filter([(_.content_has_been_scraped == 0) | _.content_has_been_scraped.isnull()])
    .select([_.url])
    .execute()
)

## Function to Scrape Article Body with Automatic Retries
@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=2, max=10))
def scrape_article_body(url):
    """Scrape the article body from a given URL using Trafilatura."""
    downloaded = trafilatura.fetch_url(url)

    if downloaded:
        extracted = trafilatura.extract(downloaded, include_comments=False, include_tables=False)
        return extracted.strip() if extracted else None
    
    return None  # If failed

## Process Each Press Release
for index, row in unscraped_press_releases.iterrows():
    url = row["url"]
    print(f"Scraping: {url}")

    try:
        article_body = scrape_article_body(url)
        
        if article_body:
            print(f"✅ Successfully scraped {url}")

            # Update database: Mark as scraped + store content
            dbx = dataset.connect(db_uri)
            dbx['statements'].upsert(
                {
                    'url': url,
                    'content_has_been_scraped': 1,
                    'text': article_body,
                },
                'url'
            )
            dbx.engine.dispose(); dbx.close()

        else:
            print(f"❌ Failed to scrape {url}")

    except Exception as e:
        print(f"⚠️ Error scraping {url}: {e}")


    time.sleep(random.uniform(5, 15))
