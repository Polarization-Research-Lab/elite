import os, urllib, time
import urllib.request

import parsel
import dotenv
import dataset
import json5
from playwright.sync_api import sync_playwright
import requests

import utils

dotenv.load_dotenv('../env')
dotenv.load_dotenv(os.environ['PATH_TO_SECRETS'])
db = f"{os.environ['DB_DIALECT']}://{os.environ['DB_USER']}:{urllib.parse.quote(os.environ['DB_PASSWORD'])}@localhost:{os.environ['DB_PORT']}/elite"



url = "https://moskowitz.house.gov/press-releases"




headers = utils.headers
headers['Referer'] = url

with dataset.connect(db) as dbx:
    legislator = dbx['scraper_press_releases'].find_one(press_release_url = url)

# legislator['article_selector'] = "//div[@class='evo-view-evo-press-releases evo-view-wrapper']/div[@class='evo-views-row-container']/div[contains(@class, 'views-row')]"
# legislator['article_selector'] = "//div[@class='evo-view-evo-press-releases evo-view-wrapper']//div[@class='views-row evo-views-row']"

# legislator['date_selector'] = "//div[contains(@class, 'theme-shape')]//div[contains(@class, 'utility-font') and contains(@class, 'label')]/div[1]"
# legislator['date_selector'] = "//li/br[1]/following-sibling::text()[1]"

# legislator['link_selector'] = "//div[@class='media evo-media-object mt-4 mb-4']//div[@class='media-body']//div[@class='h3 mt-0 font-weight-bold']/a/@href"
# legislator['link_selector'] = "//h3[contains(@class, 'jet-listing-dynamic-link')]/a/@href"

# legislator['pagination_selector'] = "//li[@class='active']/following-sibling::li[1]/a"
# legislator['pagination_selector'] = "//a[contains(@class, 'flex justify-center')]/button[contains(@class, 'text-primary')]/div/span[text()='See More']"

# legislator['js_required_for_initial_pageload'] = 1
# legislator['js_required_for_initial_pageload'] = 0
# legislator['js_required_for_pagination'] = 1

# # # # Save
# with dataset.connect(db) as dbx:
#     dbx['scraper_press_releases'].update(legislator, 'id')
# exit()
# # # # 

print(url)
print(f'''

Selectors:
article: {legislator['article_selector']}
link: {legislator['link_selector']}
date: {legislator['date_selector']}
paginator: {legislator['pagination_selector']}


js page load: {legislator['js_required_for_initial_pageload']}
js paginate: {legislator['js_required_for_pagination']}

''')

def get_elements(html):
    html_sel = parsel.Selector(text=html)
    articles = html_sel.xpath(legislator['article_selector']).getall()

    if utils.count_unions(legislator['article_selector']) > 0: 
        articles = [''.join(articles[i:i + utils.count_unions(legislator['article_selector']) + 1]) for i in range(0, len(articles), utils.count_unions(legislator['article_selector']) + 1)]

    # for article in articles: 
    #     print(article, '\n*********************\n')
    # exit()
    # print(articles[0])

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

# Test
print('\n=======\nTEST\n=========\n')
if legislator['js_required_for_pagination']:
    print('JS REQUIRED')
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()
        context.set_extra_http_headers(headers)

        page = context.new_page()
        page.goto(url)
        page.wait_for_load_state('networkidle')

        initial_content = page.content()

        html = page.content()

        print('output before pagination:')
        [link, date, pagination] = get_elements(html)

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

        [link, date, pagination] = get_elements(html)

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

        [link, date, pagination] = get_elements(html)

else:
    print('NO JS')
    if legislator['pagination_selector'].endswith('/@href') == False: legislator['pagination_selector'] += '/@href'

    html = utils.fetch_html(url, js_load = legislator['js_required_for_initial_pageload'])
    # with urllib.request.urlopen(urllib.request.Request(url, headers = headers)) as response:
        # html = response.read().decode('utf-8')

    # print(html)
    # exit()

    print(f'output before pagination (for url {url}):')
    [link, date, pagination] = get_elements(html)

    url = urllib.parse.urljoin(url, pagination)

    html = utils.fetch_html(url, js_load = legislator['js_required_for_initial_pageload'])

    # with urllib.request.urlopen(urllib.request.Request(url, headers = headers)) as response:
        # html = response.read().decode('utf-8')

    print(f'output after pagination (for new url {url}):')
    [link, date, pagination] = get_elements(html)

    url = urllib.parse.urljoin(url, pagination)

    html = utils.fetch_html(url, js_load = legislator['js_required_for_initial_pageload'])

    # with urllib.request.urlopen(urllib.request.Request(url, headers = headers)) as response:
        # html = response.read().decode('utf-8')

    print(f'output after pagination (for new url {url}):')
    [link, date, pagination] = get_elements(html)
