# Python Standard Library
import os, urllib, json, datetime

# External Dependencies
import dotenv
import ibis 
from ibis import _
import dataset
import pandas as pd 
import numpy as np
import json5
from ibis import window
import requests
from urllib.parse import urlparse

# # # # # # # # # # # 
# SETUP
# # # # # # # # # # #
# load credentials to os env
dotenv.load_dotenv('../../../env'); dotenv.load_dotenv(os.environ['PATH_TO_SECRETS'])
db_params_elite = f"{os.environ['DB_DIALECT']}://{os.environ['DB_USER']}:{urllib.parse.quote(os.environ['DB_PASSWORD'])}@localhost:{os.environ['DB_PORT']}/elite"

def save_image(leg):

    # Extract the image URL from the leg dictionary
    image_url = leg['openstates_data'].get('image') if leg.get('openstates_data') else None
    
    if not image_url:
        return None  # Return None if no image URL is found

    # Extract the file extension from the image URL
    parsed_url = urlparse(image_url)
    file_extension = os.path.splitext(parsed_url.path)[-1]  # Gets the file extension, e.g., .jpg, .png

    # Ensure the file extension is valid
    if not file_extension:
        return None  # Return None if the URL has no valid extension

    # Use leg['openstates_id'] as the filename with the extracted extension
    image_filename = os.path.join("set/", f"{leg['openstates_id'].replace('ocd-person/','')}{file_extension}")

    # Check if the file already exists
    if os.path.exists(image_filename):
        print(f"File already exists: {image_filename} | updating entry")
        return image_filename  # Return the existing filename

    if leg['img_download_attempted'] == 1: # <-- if we already attempted, stop attempting
        print(f'already attempted {image_filename}')
        return None

    try:
        # Download the image
        response = requests.get(image_url, timeout = 15)
        response.raise_for_status()  # Raise an exception for HTTP errors

        # Check if the response is actually an image
        content_type = response.headers.get('Content-Type', '')
        if not content_type.startswith('image/'):
            raise ValueError(f"Invalid content type: {content_type}")

        # Save the image to the local folder
        with open(image_filename, "wb") as image_file:
            image_file.write(response.content)

        print(f'successfully downloaded {image_filename}')
        return image_filename  # Return the path to the saved image

    except Exception as e:
        print(f"!!! Failed to download image {image_filename} !!!: \n {e}\n!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        dbx = dataset.connect(db_params_elite)
        dbx['officials'].upsert(
            {
                'openstates_id': leg['openstates_id'],
                'img_download_attempted': 1,
            },
            'openstates_id' 
        ) 
        dbx.engine.dispose(); dbx.close()
        return None


# # # # # # # # # # # 
# RUN
# # # # # # # # # # #
conn = ibis.mysql.connect(
    host = os.environ['DB_HOST'],
    user = os.environ['DB_USER'],
    password = os.environ['DB_PASSWORD'],
    database = 'elite', 
)
openstates = conn.table('openstates').select([_.openstates_id, _.openstates_data])

officials = (
    conn.table('officials')
    .filter([_['active'] == 1, _['level'] == 'state'])
)
    
officials = (
    officials
    .join(
        openstates,
        openstates['openstates_id'] == officials['openstates_id'],
        how = 'inner'
    )
    .execute()
)

officials.apply(save_image, axis = 1)


