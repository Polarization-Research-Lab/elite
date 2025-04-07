'''
---
title: House and Senate Floor Speech Ingester
---


NOTES:
- right now all we pull are media keys, since it's a pain to get the media url. however, this requires a second api call. if we start hitting monthly rate limits, we can probably reduce some of it by getting all media urls, and then matching with keys that we find in the tweet pull, so we're not double pulling. but that's going to be a headache because urls are not provided with the actual tweet itself. unreal
'''
# Python Standard Library
import os, json, datetime, tempfile, time, requests, zipfile, io

# External Resources
import dataset
import pandas as pd
import requests
import unicodedata

def clean_text(text):
    return ''.join(c for c in text if unicodedata.category(c)[0] != 'C')  # Remove all control characters

def get_tweets_by_user(user_id, start_date, end_date, bearer_token):

    # Convert start_date and end_date to ISO 8601 format
    start_time = start_date.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_time = end_date.strftime("%Y-%m-%dT%H:%M:%SZ")
    url = f"https://api.twitter.com/2/users/{user_id}/tweets"

    headers = {
        'Authorization': f'Bearer {bearer_token}',
    }

    params = {
        'start_time': start_time,
        'end_time': end_time,
        'max_results': 10,  
        'tweet.fields': 'created_at,public_metrics',  # Add this line
        'exclude': 'retweets',  # Exclude retweets
        'expansions': 'attachments.media_keys',  # Expand media attachments
        'media.fields': 'media_key,type,url',  # Fields for media attachments
    }

    # prep = requests.Request('GET', url, headers=headers, params=params).prepare()
    # print(prep)
    # print(prep.url)
    # exit()
    all_tweets = []

    retries = 0
    while retries < 1:  # Limit the number of retries to avoid infinite loops
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            response_data = response.json()
            medias = response_data.get('includes', {}).get('media', [])
            tweets = response_data.get('data', [])
            for tweet in tweets:
                if tweet.get('attachments'):
                    tweet['media_urls'] = []
                    if tweet['attachments'].get('media_keys'):
                        for key in tweet['attachments'].get('media_keys'):
                            for media in medias:
                                if media['media_key'] == key:
                                    if media['type'] == 'photo':
                                        tweet['media_urls'].append([key, media['url']])

            if not tweets:
                break

            all_tweets.extend(tweets)

            # Pagination: get the next token for the next page of results
            next_token = response.json().get('meta', {}).get('next_token')
            if next_token:
                params['pagination_token'] = next_token
            else:
                break

        elif response.status_code == 429:  # Rate limit exceeded
            print(f"Rate limit exceeded. Waiting for 15 minutes... | message: {response.status_code},{response.text}")
            time.sleep(15 * 60 + 10)  # Wait for 15 minutes

        else:
            print(f"Failed to fetch tweets. Status code: {response.status_code}")
            print(response.text)
            retries += 1
            # Exponential backoff: sleep for 2^retries seconds
            time.sleep(2**retries)
    
    return all_tweets

def ingest(legislator, start_date, end_date, db, logdb, api_key):

    # for i in range(start_date, end_date, datetime.timedelta(days = 1)):
    start_datetime = datetime.datetime.combine(start_date, datetime.datetime.min.time())
    end_datetime = datetime.datetime.combine(end_date, datetime.datetime.max.time())

    count = 0

    entries = []
    if legislator['twitter_id']:
        for twitter_id in legislator['twitter_id'].split(','):
            tweets = get_tweets_by_user(
                twitter_id, 
                start_datetime, 
                end_datetime, 
                bearer_token = api_key, 
            )

            for tweet in tweets:
                count += 1

                if legislator['level'] == 'national':
                    entries.append({
                        'date': datetime.datetime.strptime(tweet['created_at'], "%Y-%m-%dT%H:%M:%S.%fZ").date(),
                        'bioguide_id': legislator['bioguide_id'],
                        'text': clean_text(tweet['text']),
                        'tweet_id': tweet['id'],
                        'created_at': datetime.datetime.strptime(tweet['created_at'], "%Y-%m-%dT%H:%M:%S.%fZ"),
                        'public_metrics': tweet['public_metrics'],
                        'media': tweet.get('attachments'),
                        'twitter_id': twitter_id,
                        'media_urls': tweet.get('media_urls') if tweet.get('media_urls') else None,
                    })
                elif legislator['level'] == 'state':
                        entries.append({
                            'date': datetime.datetime.strptime(tweet['created_at'], "%Y-%m-%dT%H:%M:%S.%fZ").date(),
                            'openstates_id': legislator['openstates_id'],
                            'text': clean_text(tweet['text']),
                            'tweet_id': tweet['id'],
                            'created_at': datetime.datetime.strptime(tweet['created_at'], "%Y-%m-%dT%H:%M:%S.%fZ"),
                            'public_metrics': tweet['public_metrics'],
                            'media': tweet.get('attachments'),
                            'twitter_id': twitter_id,
                            'media_urls': tweet.get('media_urls') if tweet.get('media_urls') else None,
                        })
                else:
                    print(f"LEGISLATOR LEVEL IS WRONG! {legislator['name']} | {legislator['level']}")

    if entries:

        # DIFFERENTIATE BETWEEN STATE AND NATIONAL <-- EVENTUALLY JUST LUMP ALL THESE TOGETHER
        dbx = dataset.connect(db)

        if legislator['level'] == 'national':
            # upsert tweets
            dbx['tweets'].upsert_many(entries, ['tweet_id'])

            # upsert media
            media_entries = [entry for entry in entries if entry.get('media_urls')]
            if media_entries:

                media_entries = pd.DataFrame(media_entries)[['media_urls','tweet_id']]
                media_entries = media_entries.explode('media_urls')
                media_entries['media_key'] = media_entries['media_urls'].apply(lambda x: x[0])
                media_entries['url'] = media_entries['media_urls'].apply(lambda x: x[1])


                media_entries = media_entries[['tweet_id','url','media_key']]
                dbx['tweets_media'].upsert_many(
                    media_entries.to_dict(orient = 'records'),
                    ['media_key']
                )

        elif legislator['level'] == 'state':
            dbx['tweets_state'].upsert_many(entries, ['tweet_id'])

        else:
            print(f"what's wrong with the level attr of {legislator['first_name']} {legislator['last_name']}? level attr: {legislator['level']}")

        dbx.engine.dispose(); dbx.close()


    # print('count:', count)


def get_tweets_by_tweet_id(tweet_id, bearer_token):
    print('TWEET ID:', tweet_id)
    url = f"https://api.twitter.com/2/tweets/{tweet_id}"

    headers = {
        'Authorization': f'Bearer {bearer_token}',
    }

    params = {
        'tweet.fields': 'created_at,public_metrics',  # Add tweet fields
        'expansions': 'attachments.media_keys',  # Expand media attachments
        'media.fields': 'media_key,type,url',  # Fields for media attachments
    }

    retries = 0
    while retries < 2:  # Limit retries to avoid infinite loops
        response = requests.get(url, headers=headers, params=params)
        respjson = response.json()
        if response.status_code == 200:
            if respjson.get('errors'):
                pass
                # print(respjson.get('errors'))
            else:
                return respjson  # Return the full tweet data
        elif response.status_code == 429:  # Rate limit exceeded
            print(f"Rate limit exceeded. Waiting for 15 minutes...")
            time.sleep(15 * 60 + 10)  # Wait for 15 minutes
        else:
            print(f"Failed to fetch tweet. Status code: {response.status_code}")
            # Exponential backoff: sleep for 2^retries seconds
            time.sleep(2**retries)
        retries += 1

    if respjson: 
        return respjson
    else: 
        return None  # Return None if all retries fail
