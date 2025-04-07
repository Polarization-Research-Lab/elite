---
title: Legislator Efficacy
---

Pulls data from voteview to get a sense of how often legislators vote on issues and sponsor bills.

# How it works:

- Data is downloaded with `ingest.py`
- Processed with `digest.py` (sends data to internal lab database)
- `update` runs both scripts

Source: https://www.propublica.org/datastore/dataset/congressional-data-bulk-legislation-bills