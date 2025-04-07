'''
NOTE! A lot of the images saved to s3 were moved from a pipeline that migrated from database storage to s3. 
But, they weren't unzipped during the move. 
So, for many of the images in the s3 bucket, you need to unzip them with python's zlib library.
'''
import sys, json, urllib, datetime, os, time, zlib
from urllib.parse import urlparse

import dotenv
import requests
import ibis
from ibis import _
import backoff
import dataset
import pandas as pd 
import boto3

def backoff_handler(details):
    print(f"Backing off {details['wait']} seconds after {details['tries']} tries. Exception: {details['exception']}")

## Database
dotenv.load_dotenv('../../env')
dotenv.load_dotenv(os.environ['PATH_TO_SECRETS'])
params = f"{os.environ['DB_DIALECT']}://{os.environ['DB_USER']}:{urllib.parse.quote(os.environ['DB_PASSWORD'])}@localhost:{os.environ['DB_PORT']}/elite"
conn = ibis.mysql.connect(
    host=os.environ['DB_HOST'],
    user=os.environ['DB_USER'],
    password=os.environ['DB_PASSWORD'],
    database='elite',
)
tweets_media = conn.table('tweets_media')

## S3
S3_BUCKET_NAME = "prl-twitter-images"
s3_client = boto3.client("s3")


### NEW: Backoff-wrapped function for downloading the image ###
@backoff.on_exception(backoff.expo, Exception, max_tries=3, on_backoff=backoff_handler, factor=5)
def image_downloader(image_url):
    """ Downloads image from URL and returns the response object. Raises an error if request fails. """
    response = requests.get(image_url, stream=True)

    # Raise an error if status code is not 200
    if response.status_code != 200:
        raise Exception(f"Failed to download {image_url}, status code: {response.status_code}")

    return response


### Main function (no backoff, only calls image_downloader) ###
def download_image(row):
    image_url = row['url']
    media_key = row['media_key']

    print(f"Processing image: {image_url}")

    # Get file extension from URL
    parsed_url = urllib.parse.urlparse(image_url)
    file_extension = os.path.splitext(parsed_url.path)[1]  # Extracts '.jpg', '.png', etc.

    # Default to .jpg if extension is missing or invalid
    if not file_extension or len(file_extension) > 5:
        print(f'UNKNOWN FILE EXTENSION: {file_extension}')
        return 0

    # Set S3 filename
    s3_filename = f"{media_key}{file_extension}"

    ### Check if file already exists in S3 ###
    try:
        s3_client.head_object(Bucket=S3_BUCKET_NAME, Key=s3_filename)
        print(f"File already exists in S3: {s3_filename}, skipping download.")
        return 1  # File exists, mark as saved
    except s3_client.exceptions.ClientError as e:
        # If the error is 404 (Not Found), continue to download the image
        if e.response['Error']['Code'] != "404":
            print(f"Error checking S3: {e}")
            return 0  # Return failure if it's another error

    try:
        # Call image_downloader() (wrapped in backoff)
        response = image_downloader(image_url)

        # Upload to S3
        s3_client.upload_fileobj(response.raw, S3_BUCKET_NAME, s3_filename)

        # print(f"Uploaded to S3 as: {s3_filename}")
        return 1  # Success

    except Exception as e:
        print(f"Error downloading/uploading image: {e}")
        return 0  # Failure


# # # # # # # # # # # # # # #
# RUN
# # # # # 
joined = (
    tweets_media
    .filter([(_.saved != 1) | (_.saved.isnull())])
    .filter([_.url.notnull()])
    .execute()
)

chunk_size = 50
for start in range(0, len(joined), chunk_size):
    print(f'chunk: {start}')

    chunk = joined.iloc[start:start + chunk_size]
    chunk['saved'] = chunk.apply(download_image, axis=1, result_type='expand')

    dbx = dataset.connect(params)
    dbx['tweets_media'].upsert_many(
        chunk[['saved','id']].to_dict(orient='records'),
        'id'
    )
    dbx.engine.dispose(); dbx.close()
