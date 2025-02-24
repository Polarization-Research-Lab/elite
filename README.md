---
title: PRL Elite Monorepo
description: All(most) of the code the PRL uses for curating data on U.S. elected officials
---

# About

We try to keep our code as open as possible, with our primary monorepo public by default.

- `attendance`: pulls [voteview](https://voteview.com/) data to see how often legislators show up to vote; pushed to database
- `efficacy`: pulls from a [Propublica](https://www.propublica.org/) bulk data file to see how legislators vote (and how productive they are)
- `floor`: pulls floor speech data from a [congress.gov api](https://api.congress.gov/)
    - ^ curated with code lifted directly from [Nick Judd's Congressionrecord Repo](https://github.com/unitedstates/congressional-record)
- `ideology`: calculate ideology scores based on [voteview](https://voteview.com/) voting records
- `money`: pulls `fec.gov` bulk data on election donations
- `newsletters`: pulls newsletters from [DC Inbox](https://www.dcinbox.com/)
- `officials`: code for curating the profile data of federal and state legislators
    - for federal legislators: we rely primarily on the [unitedstates/congress-legislators repo](https://github.com/unitedstates/congress-legislators)
    - for state legislators: we rely primarily on the [openstates/people repo](https://github.com/openstates/people)
- `rhetoric`: code for classifying legislators rhetoric using LLMs
- `statements`: public statements scraped from US legislators' websites
    - ^ we used to use [Propublica](https://www.propublica.org/) for this, but they shut down their API. we built our own internal tool which seems to work but will require some effort to maintain properly
- `tv`: pulling tv transcripts via [https://archive.org/](https://archive.org/)
- `twitter`: pulling legislators tweets via the [Twitter API](https://developer.x.com/en/docs/x-api)

Most of this code interacts with our internal database, so much of it will be pretty useless on it's own. Making this public is more for the sake of transparency than as a utility for other researchers. However, we try to compartmentalize things in a way that hopefully makes things easy for other people to use.

Feel free to reach out to the lab if you have any questions / comments. We're always happy to discuss.

# Notes

- We utilize a file @ `./env` hidden from the public repo by default (for obvious reasons)







