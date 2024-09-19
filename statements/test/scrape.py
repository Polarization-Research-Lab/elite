# py stdlib
import os, urllib, re
from urllib.parse import urljoin

# external
import numpy as np 
import pandas as pd
import dataset
import ibis
from ibis import _
import dotenv
import requests

# internal
from actions import get_press_release_url
import utils

prompt_get_articles = """
Your job is to extract correctly find all of the available "press releases" from a given block of html.

We looking through the official websites of U.S. politicians. Each webpage has a link to "Press Releases" somewhere in the html -- often in the navigation header. However, not all politicians' websites use the same format, so we don't know for sure where it will be.

Read through the following list of urls (extracted from an html file), and infer where the link to the Press Releases page is. Your response should be the url to the Press Releases page, and nothing else. Just respond with the url.

The main site url is {url}.

The html:

```html
{html}
```

What is the full url for the Press Releases page? (if there isn't one, return "None"). Just return the url, no other text.

Your answer:
"""





# ## feed through gpt
# legislators['url_press_release'] = legislators.apply(lambda l: get_press_release_url.get(l['url']), axis = 1)
# # legislators[['id','full_name','url','url_press_release']].to_csv('withPRESS.csv', index = None)
# # legislators = pd.read_csv('withPRESS.csv')

# # ## pull instance of url
# legislators['url_press_release'] = legislators['url_press_release'].apply(lambda url: utils.re_search_url(url))

# # ## prepend local urls with base url
# legislators['url_press_release'] = legislators.apply(
#     lambda l: urljoin(l['base_url'], l['url_press_release']) if l['url_press_release'] and l['url_press_release'].startswith('/') else l['url_press_release'],
#     axis=1
# )

# ## check if url is valid
# legislators['url_press_release'] = legislators.apply(lambda l: utils.validate_url_endpoint(l['url_press_release']), axis = 1)
# # legislators.to_csv('withValidUrl.csv', index = None)
# # legislators = pd.read_csv('withValidUrl.csv')


# # ## Save
# legislators = legislators.where(pd.notna(legislators), None)
# with dataset.connect(params) as dbx:
#     dbx['legislators'].upsert_many(
#         legislators[['id', 'url_press_release']].to_dict(orient = 'records'),
#         'id',
#     )
