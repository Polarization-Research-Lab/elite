import time, functools, re
from urllib.parse import urlparse

import openai, tiktoken
from bs4 import BeautifulSoup
import requests

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

@exponential_backoff(max_retries = 3)
def fetch_html(url, max_redirects=5):
    '''
    this func fetches the html from a url, but has the characteristic where, if the url is just a redirect link, it tries to get the final endpoint html (going down max of 5 levels deep, to avoid an infinite while loop)
    '''
    try:
        for _ in range(max_redirects):
            response = requests.get(url)
            response.raise_for_status()  # Raises HTTPError for bad requests

            # Parse the HTML content
            soup = BeautifulSoup(response.text, 'html.parser')

            # Check for meta refresh tag
            meta_refresh = soup.find('meta', attrs={'http-equiv': 'refresh'})
            if meta_refresh:
                content = meta_refresh.get('content', '')
                if 'URL=' in content:
                    # Extract the new URL
                    url = content.split('URL=')[1].strip()
                    continue
            break

        return response.text
    except requests.RequestException as e:
        return str(e)

# def fetch_html(url):
#     response = requests.get(url)
#     response.raise_for_status()  # Raises HTTPError for bad requests
#     return response.text

def re_search_url(url):
    match = re.compile(r'https?://[^\s/$.?#].[^\s]*').search(url)
    if match:
        return match.group(0)
    else:
        return None

# @exponential_backoff(max_retries = 3)
def llm(message, model = 'gpt-3.5-turbo'): # chatgpt
    messages = [{
        'role': 'user',
        'content': message,
    }]

    with openai.OpenAI() as client:
        response = client.chat.completions.create(
            # model = "gpt-3.5-turbo",
            # model = "gpt-3.5-turbo-1106",
            # model = "gpt-4-1106-preview",
            # model = "gpt-4-turbo",
            # model = "gpt-4o",
            # model = "gpt-3.5-turbo",
            model = model,
            messages = messages,
            # temperature = 0.8,
            max_tokens = 50,
        )
        response = response.choices[0].message.content
    return response

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
    return len(tiktoken.encoding_for_model(model).encode(text))

def split_text_by_token_size(text, max_tokens, prompt_size, model = 'gpt-3.5-turbo'):

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






# def check_for



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




# def remove_nonbody(html_content):
#     # Parse the HTML content
#     soup = BeautifulSoup(html_content, 'html.parser')
    
#     # Extract content from the <body> tag
#     body_content = soup.find('body')
#     if body_content:
#         # Remove script tags from the body
#         for script in body_content.find_all('script'):
#             script.decompose()
        
#         # Remove style tags from the body
#         for style in body_content.find_all('style'):
#             style.decompose()

#         # Remove link tags for CSS files from the body
#         for link in body_content.find_all('link', {'rel': 'stylesheet'}):
#             link.decompose()

#         # Remove all style attributes from any tag within the body
#         for tag in body_content.find_all(style=True):
#             del tag['style']

#         # Return the modified body as a string
#         return str(body_content)
#     else:
#         # Return empty string if no body tag is present
#         return ''

