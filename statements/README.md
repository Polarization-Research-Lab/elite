---
title: Statements Ingestor
---

Execute `run` to trigger the url ingestion and article content scraping.

# How it works

## 1 - Ingest Urls

For each official in our `officials` table:

- Open a playwright process
- Use llm to pull URLs from the press release page (find press release page url if none exists)
- While min date on webpage > max date on database table of existing articles:
    - attempt to paginate
    - use llm to pull urls again

## 2 - Scrape Article Content from Each Article URL

Once we've pulled the urls from the press release page: pull the actual article content using trafilatura