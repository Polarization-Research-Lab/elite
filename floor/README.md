---
title: Floor Speech Data Ingestor
---

# How it works:

- `ingest.py` managed the data collection
- `ingestor.py` manages interacting with US Gov to pull data
- `ingest` runs the collection process

Sources:

- data comes from the [US gov's congress website](https://www.congress.gov/)
- the [congressionalrecord.py](./congressionalrecord.py) is lifted from Nick Judd's [congressional-record module](https://github.com/unitedstates/congressional-record)