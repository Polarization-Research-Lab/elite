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
from playwright.sync_api import sync_playwright
import parsel

# Internal Resources
import utils

dotenv.load_dotenv('../env')
dotenv.load_dotenv(os.environ['PATH_TO_SECRETS'])
db = f"{os.environ['DB_DIALECT']}://{os.environ['DB_USER']}:{urllib.parse.quote(os.environ['DB_PASSWORD'])}@localhost:{os.environ['DB_PORT']}/elite"

with dataset.connect(db) as dbx:
    officials = pd.DataFrame(dbx['officials'].find(level = 'national', active = True))
    press_release_scraping = pd.DataFrame(dbx['scraper_press_releases'])

officials = officials.merge(
    press_release_scraping,
    left_on = 'id',
    right_on = 'official_id',
)
officials = officials[officials['last_run_success'] != 1]
# officials = officials[officials['last_run_error'] == "No pagination found; returned: None"]
# officials = officials[officials['last_run_error'] == "LINKS ARE THE SAME; WE FAILED"]
# officials = officials[officials['last_run_error'] == "No link found; returned: None"]
# print(officials.shape[0]); exit()

# officials.to_csv('l.csv', index = None)
# officials = pd.read_csv('l.csv')

def get_elements(legislator, html):
    html_sel = parsel.Selector(text=html)
    articles = html_sel.xpath(legislator['article_selector']).getall()

    if utils.count_unions(legislator['article_selector']) > 0: 
        articles = [''.join(articles[i:i + utils.count_unions(legislator['article_selector']) + 1]) for i in range(0, len(articles), utils.count_unions(legislator['article_selector']) + 1)]

    # for article in articles: 
    #     print(article, '\n*********************\n')
    # exit()

    assert articles, "Articles Empty"

    print(f'# articles: {len(articles)}')

    # print(articles[0])
    article = parsel.Selector(text = articles[-1])

    link = article.xpath(str(legislator['link_selector'])).get()
    assert link, f"No link found; returned: {link}"

    date = article.xpath(str(legislator['date_selector'])).get()
    assert date, f"No date found; returned: {date}"

    pagination = html_sel.xpath(str(legislator['pagination_selector'])).get()
    assert pagination, f"No pagination found; returned: {pagination}"

    print(f'''
    OUTPUT
    link: {link}     
    date: {date}
    pagination: {pagination}
    ''')

    return link, date, pagination

entries = []
for l_idx, legislator in officials.iterrows():
    url = legislator['press_release_url']

    print('\n-----------------------------------\n', legislator['full_name'], legislator['id_x'])
    try:
        if legislator['no_pagination_needed'] == 1:
            print('all articles on one page...')
            html = utils.fetch_html(url, js_load = legislator['js_required_for_initial_pageload'])
            html_sel = parsel.Selector(text=html)
            articles = html_sel.xpath(legislator['article_selector']).getall()
            assert len(articles) > 0, 'NO ARTICLES FOUND'

            entry = {
                'id': legislator['id_y'],
                'last_run_success': True,
            }

        else:

            # Test
            print('\n======= TEST =========\n')
            print(url)

            if legislator['js_required_for_pagination']:
                print('JS REQUIRED')
                with sync_playwright() as p:
                    browser = p.chromium.launch(headless=True)
                    context = browser.new_context()
                    context.set_extra_http_headers(utils.headers)

                    page = context.new_page()
                    page.goto(url)
                    page.wait_for_load_state('networkidle')

                    # initial_content = page.content()

                    html = page.content()

                    print('output before pagination:')
                    [link, date, pagination] = get_elements(legislator, html)
                    first_link = link

                    # test pagination
                    if page.query_selector(legislator['pagination_selector']).evaluate('el => el.tagName') == 'OPTION':
                        paginator_element = page.query_selector(legislator['pagination_selector'])
                        assert paginator_element, "NO PAGINATOR ELEMENT FOUND"

                        next_option_value = paginator_element.get_attribute('value')
                        paginator_select = paginator_element.evaluate_handle('el => el.closest("select")')
                        paginator_select.select_option(value=next_option_value)
                        print('paginator option selected')

                    else:
                        page.wait_for_selector(legislator['pagination_selector'])

                        paginator_element = page.query_selector(legislator['pagination_selector'])
                        assert paginator_element, "NO PAGINATOR ELEMENT FOUND"

                        paginator_element.wait_for_element_state('visible')
                        paginator_element.wait_for_element_state('enabled')
                        paginator_element.scroll_into_view_if_needed()

                        assert paginator_element.is_visible(), "paginator isn't visible"
                        assert paginator_element.is_enabled(), "paginator isn't clickable"

                        paginator_element.dispatch_event('click')
                        print('paginator clicked')

                    time.sleep(5)

                    # Wait for the new content to be fully loaded after the click
                    page.wait_for_load_state('networkidle')

                    html = page.content()
                    print('output after pagination:')

                    [link, date, pagination] = get_elements(legislator, html)
                    assert first_link != link, 'LINKS ARE THE SAME; WE FAILED'
                    second_link = link

                    # test pagination
                    if page.query_selector(legislator['pagination_selector']).evaluate('el => el.tagName') == 'OPTION':
                        paginator_element = page.query_selector(legislator['pagination_selector'])
                        assert paginator_element, "NO PAGINATOR ELEMENT FOUND"

                        next_option_value = paginator_element.get_attribute('value')
                        paginator_select = paginator_element.evaluate_handle('el => el.closest("select")')
                        paginator_select.select_option(value=next_option_value)
                        print('paginator option selected')

                    else:
                        page.wait_for_selector(legislator['pagination_selector'])

                        paginator_element = page.query_selector(legislator['pagination_selector'])
                        assert paginator_element, "NO PAGINATOR ELEMENT FOUND"

                        paginator_element.wait_for_element_state('visible')
                        paginator_element.wait_for_element_state('enabled')
                        paginator_element.scroll_into_view_if_needed()

                        assert paginator_element.is_visible(), "paginator isn't visible"
                        assert paginator_element.is_enabled(), "paginator isn't clickable"

                        paginator_element.dispatch_event('click')
                        print('paginator clicked')

                    time.sleep(5)

                    # Wait for the new content to be fully loaded after the click
                    page.wait_for_load_state('networkidle')

                    html = page.content()
                    print('output after pagination:')

                    [link, date, pagination] = get_elements(legislator, html)
                    assert second_link != link, 'LINKS ARE THE SAME; WE FAILED'
                    assert first_link != link, 'LINKS ARE THE SAME; WE FAILED'

            else:
                print('NO JS')
                if legislator['pagination_selector'].endswith('/@href') == False: legislator['pagination_selector'] += '/@href'

                html = utils.fetch_html(url, js_load = legislator['js_required_for_initial_pageload'])
                print(f'output before pagination (for url {url}):')
                [link, date, pagination] = get_elements(legislator, html)
                first_link = link


                url = urllib.parse.urljoin(url, pagination)

                html = utils.fetch_html(url, js_load = legislator['js_required_for_initial_pageload'])
                print(f'output after pagination (for new url {url}):')
                [link, date, pagination] = get_elements(legislator, html)
                assert first_link != link, 'LINKS ARE THE SAME; WE FAILED'
                second_link = link


                url = urllib.parse.urljoin(url, pagination)

                html = utils.fetch_html(url, js_load = legislator['js_required_for_initial_pageload'])
                print(f'output after pagination (for new url {url}):')
                [link, date, pagination] = get_elements(legislator, html)
                assert second_link != link, 'LINKS ARE THE SAME; WE FAILED'
                assert first_link != link, 'LINKS ARE THE SAME; WE FAILED'
        
            entry = {
                'id': legislator['id_y'],
                'last_run_success': True,
                'last_run_error': '',
            }

    except Exception as e:
        print(e)
        entry = {
            'id': legislator['id_y'],
            'last_run_success': False,
            'last_run_error': e,
        }

        # raise(e)

    # Save to Database
    with dataset.connect(db) as dbx:
        dbx['scraper_press_releases'].upsert(entry, ['id'])


exit()

