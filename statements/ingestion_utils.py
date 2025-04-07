import os, sys, io, time
from urllib.parse import urljoin, urlparse
import concurrent.futures
from functools import wraps

import json5
import dotenv
import pandas as pd 
import dataset
import ibis; from ibis import _
import requests
from bs4 import BeautifulSoup
from lxml import etree
from playwright.sync_api import sync_playwright
from scrapegraphai.graphs import SmartScraperGraph

dotenv.load_dotenv('../env')
dotenv.load_dotenv(os.environ['PATH_TO_SECRETS'])

graph_config = {
    "llm": {
        "api_key": os.environ['OPENAI_API_KEY'],
        "model": "openai/gpt-4o",
    },
    "verbose": False,
    # "verbose": True,
    "headless": True,
    # "loader_kwargs": {
        # "requires_js_support": True,
        # "timeout": 10,  # Increase timeout
    # }
}


def extract_visible_html(html):
    print('*************************************** pulling visible HTML ***************************************')
    soup = BeautifulSoup(html, "html.parser")

    # Remove unnecessary elements
    for element in soup(["script", "style", "meta", "link", "head", "noscript"]):
        element.extract()  # Completely remove these elements

    # Remove hidden elements (inline styles)
    for element in soup.find_all(style=True):
        style = element["style"].lower()
        if "display: none" in style or "visibility: hidden" in style:
            element.extract()  # Remove elements that are hidden

    # Remove elements that are outside viewport (optional)
    for element in soup.find_all(attrs={"aria-hidden": "true"}): 
        element.extract()  # Removes elements explicitly marked as hidden

    # Return the cleaned HTML
    return str(soup)



def retry_on_failure(max_attempts=3):
    """Decorator that retries a function if it fails or returns None."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_attempts):
                try:
                    result = func(*args, **kwargs)
                    if result is not None:  # If the function succeeds and returns something, return it
                        return result
                except Exception as e:
                    print(f"⚠️ Attempt {attempt + 1} failed for {func.__name__}: {e}")
            print(f"❌ All {max_attempts} attempts failed for {func.__name__}")
            return None  # Return None if all attempts fail
        return wrapper
    return decorator



class DualLogger:
    def __init__(self):
        """Initialize logger with a buffer and preserve original stdout."""
        self.log_buffer = None
        self.terminal = sys.stdout  # Store original stdout

    def start(self):
        """Start capturing print statements while still displaying them on screen."""
        self.log_buffer = io.StringIO()
        sys.stdout = self  # Redirect stdout to this class

    def finish(self):
        """Stop capturing and return the log content."""
        if self.log_buffer:
            sys.stdout = self.terminal  # Restore original stdout
            log_content = self.log_buffer.getvalue() if self.log_buffer else ""
            self.log_buffer.close()
            self.log_buffer = None  # Reset buffer
            return log_content  # Return captured text
        else:
            return None


    def write(self, message):
        """Write output to both terminal and log buffer."""
        self.terminal.write(message)  # Print to screen
        if self.log_buffer:
            self.log_buffer.write(message)  # Save to buffer

    def flush(self):
        """Ensure compatibility with stdout flushing."""
        self.terminal.flush()
        if self.log_buffer:
            self.log_buffer.flush()



def make_url_absolute(base_url, url):
    """Convert a relative URL to an absolute URL using the given base URL."""
    parsed_url = urlparse(url)
    
    # If the URL has a scheme (http, https), it's already absolute
    if parsed_url.scheme:
        return url  # Already absolute
    
    # Otherwise, join it with the base URL to make it absolute
    return urljoin(base_url, url)



def check_if_url_valid(url, timeout=10):
    """
    Checks if a URL is valid by making an HTTP request.
    
    Parameters:
        url (str): The URL to check.
        timeout (int): Request timeout in seconds (default is 5s).

    Returns:
        bool: True if the URL is valid, False otherwise.
    """
    try:
        response = requests.get(url, timeout=timeout)
        # A valid response is usually in the 200-399 range
        return response.status_code < 400
    except requests.exceptions.RequestException:
        return False



def is_valid_xpath(xpath):
    try:
        etree.XPath(xpath)
        return True
    except etree.XPathSyntaxError:
        return False



def update(data, on_column, table, db_uri):
    dbx = dataset.connect(db_uri)
    dbx[table].update(
        data,
        on_column
    )
    dbx.engine.dispose(); dbx.close()



@retry_on_failure(max_attempts=3)
def get_press_release_url(official):
    press_release_url = (
        SmartScraperGraph(
            source = official.government_website,
            config = graph_config,
            prompt = """You are looking at the website of a federal legislator in Congress. Somewhere on this home page, you'll find the url for the press release page (may be referred to as public statements, press, press releases, etc). Return the url for the press releases page.""",
        )
        .run()
        ['content']
    )

    if check_if_url_valid(press_release_url):
        return press_release_url



def run_smart_scraper_graph(source, config):
    """Helper function to run SmartScraperGraph in a separate thread"""
    with open("test.html",'w') as file: file.write(extract_visible_html(source))
    return (
        SmartScraperGraph(
            source = extract_visible_html(source),
            config = config,
            prompt = """
                You are extracting data from a web page that contains a paginated list of items. The page includes a navigation element that allows the user to load more results, or move on to the next page.
   
                Your task is to find the navigation button to go to the next page. Return an xpath selector that will allow a web scaper to locate that button.
            """,
        )
        .run()
        ["content"]
    )

@retry_on_failure(max_attempts=3)
def get_next_page_selector(official, source):

    # Use a separate thread to avoid Playwright event loop conflict
    with concurrent.futures.ThreadPoolExecutor() as executor:
        future = executor.submit(
            run_smart_scraper_graph, 
            source, 
            graph_config,
        )
        next_page_selector = future.result()  # Get the result after execution

    if is_valid_xpath(next_page_selector) and (next_page_selector != 'NA'):
        return next_page_selector
    else:
        print(f"\t\t❌ Invalid XPath syntax returned:  {next_page_selector}")
        return None



@retry_on_failure(max_attempts=3)
def get_all_press_releases_on_page(official, source):
    urls = (
        SmartScraperGraph(
            source = source,
            config = graph_config,
            prompt = """You are looking at the press release page of a federal US legislator in Congress. It contains a list of press releases, which will have a title, data, and sometimes even a small snippet from the press release. Return the date and url for all of the press releases shown on the page (format as a json list: [{'url': ..., 'date': ...}, {'url': ..., 'date': ...}, ...]'. The date should be formatted as YYYY-MM-DD. If you are unable to find the date, just leave it as null.""",
        )
        .run()
        ['content']
    )

    if isinstance(urls, str): urls = json5.loads(urls)

    urls = pd.DataFrame(urls)
    urls['date'] = pd.to_datetime(urls["date"], errors='coerce').dt.date
    urls['bioguide_id'] = official.bioguide_id
    urls['party'] = official.party

    # Convert to absolute urls
    urls['url'] = urls['url'].apply(
        lambda url: make_url_absolute(official.press_release_url, url)
    )

    # Check if **all** URLs are valid
    if all(check_if_url_valid(url) for url in urls['url']):
        return urls
    else:
        print("\t\t❌ One or more invalid URLs found. Returning None.")
        return None  # Return None if any URL is invalid



def ingest_new_urls_from_press_page(official, db_uri):

    print(f'Starting scrape for {official.first_name} {official.last_name} | {official.bioguide_id} | {official.government_website} | press release url: {official.press_release_url}')

    # Set up
    # - - - - - - - - - - - - - - - - - - - - -
    urls = None; error = 0; error_text = None;

    update_press_release_url = False
    update_next_page_selector = False


    ## Get Last Date with Data
    conn = ibis.mysql.connect(host = os.environ['DB_HOST'], user = os.environ['DB_USER'], password = os.environ['DB_PASSWORD'], database = 'elite')
    statements = conn.table('statements')
    max_date = statements.filter(_.bioguide_id == official.bioguide_id)['date'].max().execute().date()
    print('MAX DATE:', max_date)
    conn.disconnect()
    # max_date = datetime.date.fromisoformat("2025-03-02")
    print(f'\tMax date from existing data: {max_date}')

    ## Check if they have a press release url
    if official.press_release_url is None: 
        print('\t\tNo Press Release URL; asking llm to find it...')
        press_release_url = get_press_release_url(official)
        if press_release_url:
            print('\t\tPress Release URL Found; updating database')
            official.press_release_url = press_release_url
            update_press_release_url = True
        else:
            error = 1; error_text = 'UNABLE TO FIND PRESS RELEASE URL'
            print('❌', error_text, '❌')

    # Collect urls page by page
    # - - - - - - - - - - - - - - - - - - - - -
    if official.press_release_url:
        print(f'\t--- collecting urls from press release page {official.press_release_url}')

        ## Mount Browser
        with sync_playwright() as p:

            browser = p.chromium.launch(headless=True)  # Set headless=True to run in the background
            page = browser.new_page()

            page.goto(official.press_release_url)
            page.wait_for_load_state('networkidle')

            page_content = page.content()
            # with open('content-0.html', 'w') as file: file.write(page_content)

            ## Collect the First Page
            print('\tCollecting First Page of Press Releases')
            urls = (
                get_all_press_releases_on_page(
                    official, 
                    source = page_content
                )
            )

            if (urls is not None) and (not urls.empty):

                # assume succes
                if update_press_release_url: update(official[['press_release_url','bioguide_id']], 'bioguide_id', 'statements_scrape_params', db_uri)

                # sort urls
                urls = urls.sort_values(by=['date','url'], ascending=[True,True])
            else:
                error = 1; error_text = 'NO URLS FOUND ON PRESS RELSEASE PAGE; SOMETHING WENT WRONG'
                print('❌', error_text, '❌')
                return urls, error, error_text

            print(f'\tFirst page collected successfully; found {urls.shape[0]} urls from range {urls["date"].dropna().min()} -to- {urls["date"].max()}')

            print(urls)
            exit()
            # Begin Pagination
            # - - - - - - - - - - - - - - - - - - - - -
            print('\t=== Starting Pagination Loop')
            for i in range(30): # <-- instead of doing a While loop which risks going forever, we cap it at 20 attempts (since we should never be going that far back anyways). And when we want to break out of the for loop if we read the last date, then we do use `break`
                print(f'\t\tIteration {i} | Current Page {page.url}')

                # Finish: 
                print(f'\tChecking if minimum date ({urls["date"].dropna().min()}) is below max date ({max_date})')
                if pd.notna(max_date): 
                    if urls["date"].dropna().min() < max_date: # <-- NOTE! MAKE SURE THIS IS ALWAYS < max_date. We have to make sure that we're pulling up to the day _before_ the max date (which will have some overlap with existing items), so that we dont have any gaps in coverage
                        print(f'\tFinished. Minimum date ({urls["date"].dropna().min()}) is below max date ({max_date})')
                        break
                else:
                    print("max_date is NaT; something went wrong. ending")
                    break

                # Continue:
                else:
                    print(f'\tContinuing on. Minimum date ({urls["date"].dropna().min()}) is above or equal to max date ({max_date})')

                    ## Check if next page selector exists
                    print('\t--- Checking if page selector exists')

                    if official.next_page_selector is None:
                        print('\t\tNo next page xpath selector found; asking llm to find it...')
                        next_page_selector = (
                            get_next_page_selector(
                                official, 
                                # source = page.url,
                                source = page_content.encode("utf-8", errors="ignore").decode("utf-8")
                            )
                        )
                        if next_page_selector:
                            print(f'\t\tPage Selector Found: {next_page_selector} | updating database')
                            official['next_page_selector'] = next_page_selector
                            update_next_page_selector = True
                        else: 
                            error = 1; error_text = 'NO NEXT PAGE SELECTOR FOUND; SOMETHING WENT WRONG'
                            print('❌', error_text, '❌')
                            break

                    ## Trigger Javascript Click Event
                    print('\t--- Attempting to click to the next page')
                    if official.next_page_selector:

                        print('\t\tLocating next page button')
                        next_button = page.locator(official.next_page_selector)
                        if next_button.count() == 0:
                            for frame in page.frames:
                                element = frame.locator(official.next_page_selector)
                                if element.count() > 0:
                                    next_button = element

                        if next_button.count() == 0:
                            error = 1; error_text = 'NO NEXT PAGE BUTTON FOUND (DESPITE HAVING XPATH SELECTOR); SOMETHING WENT WRONG'
                            print('❌', error_text, '❌')
                            break

                        if next_button.count() > 10:
                            error = 1; error_text = 'OVER 10 BUTTONS FOUND; SOMETHING WENT WRONG'
                            print('❌', error_text, '❌')
                            break

                        print('\t\tIterating through located buttons and attempting to click...')
                        for i in range(next_button.count()):
                            button = next_button.nth(i)
                            try:
                                button.scroll_into_view_if_needed()
                                button.focus()
                                button.hover()
                                button.click()
                                print(f'\t\t\tButton click event dispatched successfully; but we\'ll determine if it actually worked by checking the next round of urls')
                                break
                            except Exception as e:
                                print(f'\t\t\tFailed click event: {i}')
                                pass

                        print('\t\tWaiting for network idle (plus some extra padding time)')
                        page.wait_for_load_state('networkidle')
                        time.sleep(3)

                        print('\t\tGathering new page content')
                        page_content = page.content()  # Get updated page HTML
                        # with open('content-1.html', 'w') as file: file.write(page_content)

                        print('\t\tPulling (hopefully) new urls')
                        new_urls = get_all_press_releases_on_page(
                            official, 
                            source = page_content
                        )

                        if (new_urls is not None) and (not new_urls.empty):
                            new_urls = new_urls.sort_values(by=['date','url'], ascending=[True,True])

                            print('\t\tNext page of URLS found; checking if they\'re new.')

                            # If failure:
                            if new_urls['url'].iloc[0] == urls['url'].iloc[0]:
                                error = 1; error_text = 'NEW URLS ARE NOT DIFFERENT FROM EXISTING URLS; BUTTON MUST HAVE FAILED'
                                print('❌', error_text, '❌')
                                break

                            # If success:
                            else:
                                print('\t\t\tNew URLs are different from previous urls; Assuming the button click was successful')
                                if update_next_page_selector: update(official[['next_page_selector','bioguide_id']], 'bioguide_id', 'statements_scrape_params', db_uri)

                        # If Fail:
                        else:
                            error = 1; error_text = 'NO NEW URLS FOUND; BUTTON MUST HAVE FAILED'
                            print('❌', error_text, '❌')
                            break

                        print('\t\tSuccess. Concatenating new urls with old urls')

                        urls = (
                            pd.concat([urls, new_urls])
                            .drop_duplicates(subset = ['url'])
                            .reset_index(drop = True)
                            .sort_values(by = ['date','url'], ascending = [True,True])
                        )
                        urls['date'] = pd.to_datetime(urls['date'], errors='coerce').dt.date
                        print(f'\t\tURLs now range from {urls["date"].dropna().min()} -to- {urls["date"].max()}; continuing loop')

            print('\t=== Pagination Loop finished; closing browser')
            browser.close()

    return urls, error, error_text



