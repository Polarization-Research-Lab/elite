'''
---
title: Legislator Newsletter Ingester
---
'''
# Python Standard Library
import sys, os, json, datetime, tempfile, time, csv, traceback
import urllib

# External Resources
import json5
import dotenv
import pandas as pd
import dataset
import requests
import newspaper

# Internal Resources
import scraper
import utils

dotenv.load_dotenv('../env')
dotenv.load_dotenv(os.environ['PATH_TO_SECRETS'])
tablename = 'statements'
def init(db):
    with dataset.connect(db) as dbx:
        table = dbx.create_table(tablename, primary_id = 'id', primary_type = dbx.types.integer, primary_increment = True)
        table.create_column('date', dbx.types.datetime)
        table.create_column('bioguide_id', dbx.types.string(50))
        table.create_column('text', dbx.types.text)
        table.create_column('url', dbx.types.text)
        table.create_column('title', dbx.types.text)
        table.create_column('type', dbx.types.text)
        table.create_column('congress', dbx.types.string(50))
        table.create_column('chamber', dbx.types.string(50))
        table.create_column('name', dbx.types.text)
        table.create_column('state', dbx.types.string(20))
        table.create_column('party', dbx.types.string(11))


def ingest(legislator, start_date, end_date, db, logdb):
    '''
    Ingest the Data
    '''
    dbx = dataset.connect(db)
    press_release_scraping = dbx['scraper_press_releases'].find_one(official_id = legislator['id'])
    dbx.engine.dispose(); dbx.close()

    print(legislator['name'])

    legislator_plus = pd.concat([legislator, pd.Series(press_release_scraping)])
    if legislator_plus.get('article_selector'):
        entries = []

        try:
            articles = scraper.run(
                start_date = start_date,
                end_date = end_date,

                url = legislator_plus['press_release_url'], 
                full_page_link = legislator_plus['no_pagination_needed'],
                js_on_page_load = legislator_plus['js_required_for_initial_pageload'],
                js_for_paginate = legislator_plus['js_required_for_pagination'],

                article_selector = legislator_plus['article_selector'],
                date_selector = legislator_plus['date_selector'],
                link_selector = legislator_plus['link_selector'],
                pagination_selector = legislator_plus['pagination_selector'],
            )
            if articles:

                # Check the article links are real
                for article in articles:
                    response = requests.head(article['link'])
                    if response.status_code == 404:
                        raise ValueError(f"Error: Link {article['link']} returned a 404 status code.")

                print(f'found {len(articles)} articles')
                for article in articles:

                    time.sleep(.5)

                    # parse
                    a = newspaper.Article(article['link'])
                    a.download()
                    a.parse()

                    # format entry
                    entries.append({
                        'date': article['date'],
                        'bioguide_id': legislator_plus['bioguide_id'],
                        'text': a.text,
                        'url': article['link'],
                        'title': a.title,
                        'type': None,
                        'congress': None,
                        'chamber': legislator_plus['type'],
                        'name': legislator_plus['full_name'],
                        'state': legislator_plus['state'],
                        'party': legislator_plus['party'],
                    })

                # Save to Database
                # with dataset.connect(db) as dbx:
                legislator_plus['last_run_success'] = 1
                l = legislator_plus[['last_run_success', 'official_id']].to_dict()
                dbx = dataset.connect(db)
                dbx[tablename].upsert_many(entries, ['url','date'])
                dbx['scraper_press_releases'].upsert(l, 'official_id')
                dbx.engine.dispose(); dbx.close()

        except Exception as e:
            # print(traceback.print_exc())
            print(e)
            # with dataset.connect(logdb) as logdbx:
            #     logdbx['errors'].insert({
            #             'process': f"{os.popen(f'ps -o command= -p $(ps -o ppid= -p {os.getppid()})').read().strip()} >> {os.popen(f'ps -p {os.getpid()} -o args=').read().strip()}", # <-- complicated way of getting the system command that executed the current script
            #             'tags': 'elite,rhetoric,ingest',
            #             'message': f'{traceback.print_exc()}'
            #         })
            legislator_plus['last_run_success'] = 0  
            legislator_plus['last_run_err'] = f'{e}'
            l = legislator_plus[['last_run_success', 'official_id', 'last_run_err']].to_dict()
            dbx = dataset.connect(db)
            dbx['scraper_press_releases'].upsert(l, 'official_id')
            dbx.engine.dispose(); dbx.close()




# if __name__ == '__main__':

#     import dotenv
#     dotenv.load_dotenv('../env')
#     dotenv.load_dotenv(os.environ['PATH_TO_SECRETS'])
#     db = f"{os.environ['DB_DIALECT']}://{os.environ['DB_USER']}:{urllib.parse.quote(os.environ['DB_PASSWORD'])}@localhost:{os.environ['DB_PORT']}/elite"
#     logdb = f"{os.environ['DB_DIALECT']}://{os.environ['DB_USER']}:{urllib.parse.quote(os.environ['DB_PASSWORD'])}@localhost:{os.environ['DB_PORT']}/log"

#     ingest(datetime.date(2024, 5, 10), datetime.date(2024, 7, 1), db, logdb, None)
