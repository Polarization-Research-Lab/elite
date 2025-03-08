import sys, json, urllib, datetime, os, time, zlib, re

import dotenv
import requests
import ibis
from ibis import _
import backoff
import dataset
import pandas as pd 
import openai

def backoff_handler(details):
    print(f"Backing off {details['wait']} seconds after {details['tries']} tries. Exception: {details['exception']}")

dotenv.load_dotenv('../../env')
dotenv.load_dotenv(os.environ['PATH_TO_SECRETS'])
params = f"{os.environ['DB_DIALECT']}://{os.environ['DB_USER']}:{urllib.parse.quote(os.environ['DB_PASSWORD'])}@localhost:{os.environ['DB_PORT']}/elite"
conn = ibis.mysql.connect(
    host = os.environ['DB_HOST'],
    user = os.environ['DB_USER'],
    password = os.environ['DB_PASSWORD'],
    database = 'elite',
)

tweets_media = conn.table('tweets_media')

def clean_json_string(json_str):
    # Remove any prefix like ```json\n
    json_str = re.sub(r'```json\n', '', json_str)  # Remove ```json\n if it exists
    json_str = re.sub(r'.*?{', '{', json_str, count=1)  # Remove everything before the first '{'
    json_str = re.sub(r'}.*', '}', json_str, count=1)  # Remove everything after the last '}'
        
        # Remove trailing backticks (```)
    json_str = json_str.rstrip('`')  # Removes backticks at the end
        
        # Replace smart quotes “ and ” with standard quotes "
    json_str = json_str.replace("“", '"').replace("”", '"')
        
    return json_str

# Step: Function to safely parse the cleaned JSON string
def safe_json_loads(json_str):
    cleaned_str = clean_json_string(json_str)  # Clean the JSON string
    try:
        return json.loads(cleaned_str)
    except json.JSONDecodeError:
        return {}  # Return an empty dictionary if JSON is invalid

def safe_image_query(url, prompt):
    try:
        # Attempt to run the image query
        return image_query(url, prompt)
    except Exception as e:
        # Output 'url error' in case of failure
        print(f"Error processing URL {url}: {e}")
        return 'url error'  # Return 'url error' if there is an exception

def image_query(image_url, prompt):
    with openai.OpenAI() as client:
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
              {
                "role": "user",
                "content": [
                  {"type": "text", "text": f"{prompt}"},
                  {
                    "type": "image_url",
                    "image_url": {
                      "url": f"{image_url}"},
                  },
                ],
              }
            ],
            max_tokens=300,
        )
    
    return response.choices[0].message.content

with open('prompt.txt', 'r') as file:
    prompt = file.read()

item_count = (
    tweets_media
    .filter([(tweets_media['annotated'].isnull()) | (tweets_media['annotated'] != 1), tweets_media['url'] != None])
    .count()
    .execute()
)

chunksize = 20
for i in range(0, item_count, chunksize):
    # print(': ', i)

    ## pull chunk
    end = min(i + chunksize, item_count)  # Ensures last chunk is correct

    ## Pull chunk safely
    sample = (
        tweets_media
        .filter([
            (tweets_media['annotated'].isnull()) | (tweets_media['annotated'] != 1), 
            tweets_media['url'] != None
        ])
        .limit(chunksize, offset=i)  # Correct offset without over-fetching
        .execute()
    )

    if sample.empty:  # Stop if no more data
        print("No more data to process.")
        break

    ## image processing
    # sample['raw_gpt_output'] = sample['url'].apply(lambda x: safe_image_query(x, prompt))

    sample_urls = sample[['url']] # <-- convert to dask for parallelization
    sample['raw_gpt_output'] = sample_urls['url'].apply( # <-- build apply
        lambda x: safe_image_query(x, prompt),
    )

    # Step: Apply the cleaning and parsing process
    sample['full_json_output'] = sample['raw_gpt_output'].apply(safe_json_loads)

    sample['image_description'] = sample['raw_gpt_output'].apply(
        lambda x: safe_json_loads(x).get('image_description', None) if isinstance(safe_json_loads(x), dict) else None
    )
    sample['is_text'] = sample['raw_gpt_output'].apply(
        lambda x: safe_json_loads(x).get('is_text', None) if isinstance(safe_json_loads(x), dict) else None
    )
    sample['image_text'] = sample['raw_gpt_output'].apply(
        lambda x: safe_json_loads(x).get('image_text', None) if isinstance(safe_json_loads(x), dict) else None
    )
    sample['image_objects'] = sample['raw_gpt_output'].apply(
        lambda x: safe_json_loads(x).get('image_objects', [None]) if isinstance(safe_json_loads(x), dict) else [None]
    )
    sample['image_tweet'] = sample['raw_gpt_output'].apply(
        lambda x: safe_json_loads(x).get('image_tweet', None) if isinstance(safe_json_loads(x), dict) else None
    )

    sample['annotated'] = sample['image_description'].apply(
        # lambda x: 1 if sample['image_description'] else None
        lambda x: 1
    )

    sample['image_text'] = sample['image_text'].astype(str)

    # Save to db
    db = f"{os.environ['DB_DIALECT']}://{os.environ['DB_USER']}:{urllib.parse.quote(os.environ['DB_PASSWORD'])}@localhost:{os.environ['DB_PORT']}/elite" # <-- note, we assign the database to be "research" where you DO have ALTER access
    dbx = dataset.connect(db)
    dbx['tweets_media'].upsert_many(
        sample[['id', 'raw_gpt_output', 'full_json_output', 'image_description', 'is_text','image_text','image_objects','image_tweet', 'annotated']].to_dict(orient = 'records'),
        ['id']
    ) # <-- note: this will throw an error if there are any existing records with the same unique "id" as the ones you are trying to insert
    dbx.engine.dispose(); dbx.close()

print('--- done ---')
