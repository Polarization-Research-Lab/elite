---
title: PRL Elite Data
description: All of the code handling the curation and annotation (classification) of elite rhetoric (collected from various sources).
---

# Why can't I run this?

This repo contains code that's meant to run from our internal server. We publish it so that our work can be audited by the scientific community. We keep no secrets as to how our public data is acquired and curated.

So, this code isn't meant to be a grab-and-go tool for anyone to use. Maintaining this as a grab-and-go tool would also be a little burdensome for our small dev team.

That said, if you'd like to run this code yourself, this is what you'll have to do:

- create an `env` file with the following line:

```
py3={/your/path/to/python3}
PATH_TO_SECRETS="{a text file}"
```

- The file `PATH_TO_SECRETS` points to should have the following lines (filling in required values):
```bash
DB_DIALECT="*************"
DB_USER="*************"
DB_PASSWORD="*************"
DB_HOST="*************"
DB_PORT="*************"
TWITTER_API="*************"
PROPUBLICA_API="*************"
CONGRESS_API="*************"
OPENAI_API_KEY="*************"
```

- get python dependencies
```bash
$py3 -m pip install -r /python/requirements.txt
```

After all that, most scripts should work correctly
