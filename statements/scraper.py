# Imports
## Python Stanard Library
import urllib.parse
import time, datetime


## External
import requests
import pandas as pd 
import parsel
import newspaper
import dateutil
from playwright.sync_api import sync_playwright

## Internal
import utils

def scrape_articles(html, article_selector, link_selector, date_selector):
    '''
    Scrape Articles From HTML
    '''
    page_content = parsel.Selector(text=html)
    articles = page_content.xpath(article_selector).getall()
    if utils.count_unions(article_selector) > 0: 
        articles = [''.join(articles[i:i + utils.count_unions(article_selector) + 1]) for i in range(0, len(articles), utils.count_unions(article_selector) + 1)]

    links_and_dates = []

    for article in articles:
        article_content = parsel.Selector(text=article)
        link = article_content.xpath(link_selector).get()
        try:
            date = dateutil.parser.parse(article_content.xpath(date_selector).get(), fuzzy=True)
        except:
            try: # try again on just the inner html
                date = dateutil.parser.parse(article_content.xpath(date_selector).xpath('string()').get().strip(), fuzzy=True)
            except:
                date = None

        assert link, f'No Link Found | {link}'
        assert date, f'No Date Found | {date}'

        links_and_dates.append([link, date.date()])

    if not links_and_dates:
        raise Exception("ERR: NO ARTICLES FOUND")

    links_and_dates_df = pd.DataFrame(links_and_dates, columns=['link', 'date'])
    return links_and_dates_df


def run(url, article_selector, link_selector, date_selector, start_date, end_date, pagination_selector = None, full_page_link=None, js_on_page_load=False, js_for_paginate=False):
    '''
    Run Full Scraper w/ Pagination
    '''
    results = []
    base_url = url[:]
    prev_last_link = ''
    paginate = True
    tries = 0
    
    if full_page_link:
        html = utils.fetch_html(url, js_load = js_on_page_load)    
        results = [scrape_articles(html, article_selector, link_selector, date_selector)]
    else:
        # WHEN JS IS NEEDED FOR PAGINATION
        if js_for_paginate:
            with sync_playwright() as p:
                print('JS REQUIRED')
                browser = p.chromium.launch(headless=True)
                page = browser.new_page()
                page.goto(url)
                page.wait_for_load_state('networkidle')

                initial_html = page.content()

                while paginate:
                    print('paginating...')

                    html = page.content()

                    articles = scrape_articles(html, article_selector, link_selector, date_selector)
                    print(f"found {articles.shape[0]} articles from {articles['date'].min()} through {articles['date'].max()}")
                    results.append(articles)

                    if prev_last_link == articles.iloc[-1]['link']:
                        raise Exception("error on pagination: prev last link is same as current last link")

                    prev_last_link = articles.iloc[-1]['link']

                    # check if we need to paginate
                    if not articles.empty and start_date <= articles['date'].min():
                        page.wait_for_selector(pagination_selector)
                        paginator_element = page.query_selector(pagination_selector)

                        paginator_element.wait_for_element_state('visible')
                        paginator_element.wait_for_element_state('enabled')
                        paginator_element.scroll_into_view_if_needed()

                        # paginator_element.dblclick()
                        paginator_element.dispatch_event('click')
                        time.sleep(3)

                        # Wait for the new content to be fully loaded after the click
                        page.wait_for_load_state('networkidle')
                    else:
                        paginate = False

                    tries += 1
                    if tries >= 60: 
                        browser.close()
                        raise Exception("Overpaginated. Too Many Tries. Backing off to avoid getting blacklisted")

                browser.close()


        # WHEN JS IS NOT NEEDED FOR PAGINATION
        else:
            print('NO JS NEEDED')
            if pagination_selector.endswith('/@href') == False: pagination_selector += '/@href'

            while paginate:
                print('paginating...')
                response = requests.get(url)
                html = utils.fetch_html(url, js_load = js_on_page_load)

                articles = scrape_articles(html, article_selector, link_selector, date_selector)
                print(f"found {articles.shape[0]} articles from {articles['date'].min()} through {articles['date'].max()}")
                results.append(articles)

                assert prev_last_link != articles.iloc[-1]['link'], "error on pagination: prev last link is same as current last link"
                prev_last_link = articles.iloc[-1]['link']

                if (not articles.empty) and (start_date <= articles['date'].min()):
                    print(f'PAGINATING; min article date: {articles["date"].min()}; startdate: {start_date}')
                    next_page = parsel.Selector(text=html).xpath(pagination_selector).get()
                    if next_page:
                        url = urllib.parse.urljoin(base_url, next_page)
                    else:
                        raise Exception("Paginator not found")
                else:
                    paginate = False

                tries += 1
                if tries >= 60: 
                    browser.close()
                    raise Exception("Overpaginated. Too Many Tries. Backing off to avoid getting blacklisted")
                time.sleep(3)


    print(pd.concat(results))

    results = pd.concat(results)

    if results['date'].max() > datetime.date.today():
        print('DATE ERR!!!!')
        raise Exception(f"Max date found: {results['date'].max()}; current date is {datetime.date.today()} <-- that's an error; CHECK!")

    
    results = results.query('@start_date <= date <= @end_date')
    

    if not results.empty:
        results['link'] = results['link'].apply(lambda x: urllib.parse.urljoin(base_url, x))
        return pd.DataFrame(results).drop_duplicates(subset = 'link', keep = 'last').to_dict(orient = 'records')

