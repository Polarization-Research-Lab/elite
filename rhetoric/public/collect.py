import os, urllib 

import dotenv

dotenv.load_dotenv('../../env'); dotenv.load_dotenv(os.environ['PATH_TO_SECRETS'])

import pandas as pd
import ibis
from ibis import _
import dataset

# Setup
params = f"{os.environ['DB_DIALECT']}://{os.environ['DB_USER']}:{urllib.parse.quote(os.environ['DB_PASSWORD'])}@localhost:{os.environ['DB_PORT']}/web"
conn = ibis.mysql.connect(
    host=os.environ['DB_HOST'],
    user=os.environ['DB_USER'],
    password=os.environ['DB_PASSWORD'],
    database='elite',
)

officials = (
    conn.table('officials')
    .mutate(
        full_name = (_.first_name + ibis.literal(' ') + _.last_name)
    )
    .filter([_.active == 1, _.level == 'national'])
)
tweets = conn.table('tweets')
classifications = conn.table('classifications')

# Rhetoric
years = (
    classifications
    .mutate(year = _['date'].year())
    .filter([_.date >= '2023-01-01', _.classified == 1])
    .group_by('year')
    .aggregate()
    .execute()
    ['year']
    .to_list()
)
years = sorted(years)
paths = []

for year in years:

    # Filter and mutate the classifications data for each year
    year_data = (
        classifications
        .mutate(year = _['date'].year())
        .filter([_['year'] == year])
        # .limit(10) # <-- for testing (otherwise it takes a while)
    )

    # Join the year_data with officials on 'bioguide_id'
    joined_data = (
        year_data.join(officials, year_data['bioguide_id'] == officials['bioguide_id'])
        .select(
            year_data,  # select all columns from year_data
            officials['first_name'], 
            officials['last_name'], 
            officials['state'], 
            officials['type']  # select only specific columns fromofficials 
        )
    )
    

    # Join the above joined_data with tweets on 'source_id' == 'tweet_id'
    final_data = (
        joined_data.join(tweets, joined_data['source_id'] == tweets['id'])
        .select(
            joined_data,
            tweets['tweet_id']  # select only tweet_id from tweets
        )
    )

    # Mutate the tweet_id based on the source not being 'twitter'
    final_data = final_data.mutate(
        tweet_id = ibis.ifelse(final_data['tweet_id'].isnull(), '', final_data['tweet_id'])
    )

    final_data = final_data.mutate(
        url = ibis.ifelse(
            (final_data['source'] == 'tweets') & (final_data['tweet_id'] != ''), ibis.literal("https://twitter.com/00000000000/status/") + final_data['tweet_id'], None
        ),
        text = final_data['source'].cases(
            ('tweets', ibis.literal('')),
            else_=final_data['text']
        )
    )


    # Execute the query to fetch the data
    result_data = final_data.execute()
    
    result_data = result_data.drop(columns = ['errors','dictionary','valence'], axis = 1)    
    result_data.to_csv(
        f'.tmp/rhetoric/{year}.zip',
        compression = {
            'method': 'zip', 
            'archive_name': f'{year}.csv',
        }
    )

    paths.append(f'downloads/elite-data/rhetoric/{year}.zip')

meta_data_query = """
SELECT 
    COLUMN_NAME, 
    DATA_TYPE, 
    COLUMN_COMMENT 
FROM 
    INFORMATION_SCHEMA.COLUMNS 
WHERE 
    TABLE_SCHEMA = 'elite' 
    AND TABLE_NAME = '{table}';
"""

# Meta data for the classifications table
(
    pd.DataFrame(
        conn.raw_sql(meta_data_query.format(table = 'classifications')),
        columns = ['COLUMN_NAME', 'DATA_TYPE', 'DESCRIPTION'],
    )
    .query('COLUMN_NAME not in ["id", "dictionary", "errors", "valence"]')
    .to_csv('.tmp/communication-meta.csv', index = None) # <-- save schema to csv
)
paths.append('downloads/elite-data/communication-meta.csv')

# Get Aggregates
ideology = conn.table('ideology')
efficacy = conn.table('efficacy')
attendance = conn.table('attendance')
money = conn.table('money')
rhetoric = conn.table('rhetoric').filter(_.source == 'all')


ideology = ideology.rename(**{f'ideology_{col}': col for col in ideology.columns if col not in ['id', 'bioguide_id']}) 
efficacy = efficacy.rename(**{f'efficacy_{col}': col for col in efficacy.columns if col not in ['id', 'bioguide_id']}) 
attendance = attendance.rename(**{f'attendance_{col}': col for col in attendance.columns if col not in ['id', 'bioguide_id']}) 
money = money.rename(**{f'money_{col}': col for col in money.columns if col not in ['id', 'bioguide_id']}) 
rhetoric = rhetoric.rename(**{f'communication_{col}': col for col in rhetoric.columns if col not in ['id', 'bioguide_id']}) 

profiles = (
   officials 
    .select(['bioguide_id', 'full_name'])
    .left_join(ideology, ideology.bioguide_id == _.bioguide_id).drop(['id', 'bioguide_id_right'])
    .left_join(efficacy, efficacy.bioguide_id == _.bioguide_id).drop(['id', 'bioguide_id_right'])
    .left_join(attendance, attendance.bioguide_id == _.bioguide_id).drop(['id', 'bioguide_id_right'])
    .left_join(money, money.bioguide_id == _.bioguide_id).drop(['id', 'bioguide_id_right'])
    .left_join(rhetoric, rhetoric.bioguide_id == _.bioguide_id).drop(['id', 'bioguide_id_right'])
    .execute()
)

profiles.to_csv('.tmp/profiles.zip', index = None, compression = {'method': 'zip', 'archive_name': f'profiles.csv'})
paths.append('downloads/elite-data/profiles.zip')

# Get Schemas
## Pull and format
ideology = pd.DataFrame(conn.raw_sql(meta_data_query.format(table = 'ideology')), columns = ['COLUMN_NAME', 'DATA_TYPE', 'DESCRIPTION']).query('COLUMN_NAME != "id"')
efficacy = pd.DataFrame(conn.raw_sql(meta_data_query.format(table = 'efficacy')), columns = ['COLUMN_NAME', 'DATA_TYPE', 'DESCRIPTION']).query('COLUMN_NAME != "id"').query('COLUMN_NAME != "bioguide_id"')
attendance = pd.DataFrame(conn.raw_sql(meta_data_query.format(table = 'attendance')), columns = ['COLUMN_NAME', 'DATA_TYPE', 'DESCRIPTION']).query('COLUMN_NAME != "id"').query('COLUMN_NAME != "bioguide_id"')
money = pd.DataFrame(conn.raw_sql(meta_data_query.format(table = 'money')), columns = ['COLUMN_NAME', 'DATA_TYPE', 'DESCRIPTION']).query('COLUMN_NAME != "id"').query('COLUMN_NAME != "bioguide_id"')
rhetoric = pd.DataFrame(conn.raw_sql(meta_data_query.format(table = 'rhetoric')), columns = ['COLUMN_NAME', 'DATA_TYPE', 'DESCRIPTION']).query('COLUMN_NAME != "id"').query('COLUMN_NAME != "bioguide_id"')

## Prepend
ideology['COLUMN_NAME'] = ideology['COLUMN_NAME'].apply(lambda x: f'ideology_{x}' if x != 'bioguide_id' else x)
efficacy['COLUMN_NAME'] = efficacy['COLUMN_NAME'].apply(lambda x: f'efficacy_{x}')
attendance['COLUMN_NAME'] = attendance['COLUMN_NAME'].apply(lambda x: f'attendance_{x}')
money['COLUMN_NAME'] = money['COLUMN_NAME'].apply(lambda x: f'money_{x}')
rhetoric['COLUMN_NAME'] = rhetoric['COLUMN_NAME'].apply(lambda x: f'communication_{x}')

## stack
schema = pd.concat([ideology,efficacy,attendance,money,rhetoric], axis = 0, ignore_index = True)
schema.to_csv('.tmp/profiles-meta.csv', index = None)
paths.append('downloads/elite-data/profiles-meta.csv')
