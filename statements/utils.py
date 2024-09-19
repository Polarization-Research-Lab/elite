import time, functools, re
from urllib.parse import urlparse

import openai, tiktoken
from bs4 import BeautifulSoup
import requests

from playwright.sync_api import sync_playwright

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'Referer': None,
}

def is_valid_url(url):
    parsed = urlparse(url)
    return bool(parsed.scheme) and bool(parsed.netloc)

def exponential_backoff(max_retries = 3):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            retries = 0
            delay = 1  # Initial delay in seconds, you can adjust this as needed
            while retries < max_retries:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    print(f"Attempt {retries+1} failed with error: {e}")
                    time.sleep(delay)
                    retries += 1
                    delay *= 2  # Double the delay each retry
            return f"Failed after {max_retries} retries"
        return wrapper
    return decorator

# @exponential_backoff(max_retries = 3)
# def fetch_html(url, max_redirects=5):
#     '''
#     this func fetches the html from a url, but has the characteristic where, if the url is just a redirect link, it tries to get the final endpoint html (going down max of 5 levels deep, to avoid an infinite while loop)
#     '''
#     try:
#         for _ in range(max_redirects):
#             response = requests.get(url)
#             response.raise_for_status()  # Raises HTTPError for bad requests

#             # Parse the HTML content
#             soup = BeautifulSoup(response.text, 'html.parser')

#             # Check for meta refresh tag
#             meta_refresh = soup.find('meta', attrs={'http-equiv': 'refresh'})
#             if meta_refresh:
#                 content = meta_refresh.get('content', '')
#                 if 'URL=' in content:
#                     # Extract the new URL
#                     url = content.split('URL=')[1].strip()
#                     continue
#             break

#         return response.text
#     except requests.RequestException as e:
#         return str(e)




def fetch_html(url, js_load = False):
    use_these_headers = headers
    use_these_headers['Referer'] = url

    if js_load == False:
        response = requests.get(url, headers = use_these_headers)
        response.raise_for_status()  # Raises HTTPError for bad requests
        return response.text
    else:
        with sync_playwright() as p:

            # Launch the browser
            browser = p.chromium.launch(headless=True)

            # Create a new page
            page = browser.new_page()
            page.set_extra_http_headers(use_these_headers)

            # Go to the URL
            page.goto(url)

            # Wait for the content to be fully loaded
            page.wait_for_load_state('networkidle')
            
            # Get the page content
            html_content = page.content()

            # Close the browser
            browser.close()

            return html_content





def re_search_url(url):
    match = re.compile(r'https?://[^\s/$.?#].[^\s]*').search(url)
    if match:
        return match.group(0)
    else:
        return None

def extract_hrefs(html_content):
    # Extract specific parts that are more likely to include URLs
    soup = BeautifulSoup(html_content, 'html.parser')
    relevant_content = ' '.join([a['href'] for a in soup.find_all('a', href=True)])
    return relevant_content

def validate_url_endpoint(url):
    try:
        response = requests.head(url, allow_redirects=True)
        # If HEAD request is not allowed, try GET request
        if response.status_code >= 400:
            response = requests.get(url, allow_redirects=True)
        return url
    except requests.RequestException as e:
        # Any exception thrown indicates the URL is not valid
        print(requests.RequestException)
        return None

def get_token_size(text, model = 'gpt-3.5-turbo'):
    if model == 'gpt-4o': model = 'gpt-4-turbo'
    return len(tiktoken.encoding_for_model(model).encode(text))

def split_text_by_token_size(text, max_tokens, prompt_size, model = 'gpt-3.5-turbo'):
    if model == 'gpt-4o': model = 'gpt-4-turbo'

    # Get the tokenizer for the specified encoding
    encoding = tiktoken.encoding_for_model(model)
    
    # Tokenize the text
    tokens = encoding.encode(text)
    
    # Split tokens into chunks of max_tokens size
    step_size = max_tokens - prompt_size
    chunks = [tokens[i:i + step_size] for i in range(0, len(tokens), step_size)]

    # Decode the token chunks back into text
    text_chunks = [encoding.decode(chunk) for chunk in chunks]
    
    return text_chunks

def remove_css(html_content):
    # Parse the HTML content
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Remove all style tags
    for tag in soup.find_all('style'):
        tag.decompose()

    # Remove all link tags for CSS files
    for link in soup.find_all('link', {'rel': 'stylesheet'}):
        link.decompose()

    # Remove all style attributes from any tag
    for tag in soup.find_all(style=True):
        del tag['style']

    # Return the modified HTML
    return str(soup)

def remove_js(html_content):
    # Parse the HTML content
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Remove all script tags
    for script in soup.find_all('script'):
        script.decompose()

    # Return the modified HTML
    return str(soup)


def count_unions(xpath_string):
    # Regex to match literal strings in XPath (either single or double quotes)
    literal_string_pattern = re.compile(r'(["\']).*?\1')
    
    # Remove all literal strings from the XPath expression
    cleaned_xpath = literal_string_pattern.sub('', xpath_string)
    
    # Count the number of union operators '|' in the cleaned XPath expression
    return cleaned_xpath.count('|')
