import os, json, urllib, pickle, time
import csv

import dotenv
import pandas as pd
import ibis
from ibis import _
import dataset

# setup
dotenv.load_dotenv('../env')
dotenv.load_dotenv(os.environ['PATH_TO_SECRETS'])

params = f"{os.environ['DB_DIALECT']}://{os.environ['DB_USER']}:{urllib.parse.quote(os.environ['DB_PASSWORD'])}@localhost:{os.environ['DB_PORT']}/elite"


with dataset.connect(params) as dbx:
    legislators = pd.DataFrame(dbx['officials'])

ids_expanded = legislators['fec_ids'].str.split(',', expand=True).reset_index()
ids_expanded_melted = ids_expanded.melt(id_vars='index', value_name='fec_ids').drop('variable', axis=1).dropna()


# Merge expanded IDs with the original dataframe
legislators = pd.merge(ids_expanded_melted, legislators.drop('fec_ids', axis=1), left_on='index', right_index=True).drop('index', axis=1)


# legislators['fec_ids'] = legislators['fec_ids'].str.split(',').str[-1]

# DB Table Build
tablename = 'money'
with dataset.connect(params) as dbx:
    table = dbx.create_table(tablename, primary_id = 'id', primary_type = dbx.types.integer, primary_increment = True)
    table.create_column('bioguide_id', dbx.types.string(7), unique = True, nullable = False)

# Load Money Data; merge with CAND_ID (so we can merge with legislators table)

# load actual campaign contributions data
with open('assets/indiv_header_file.csv') as file: headers = file.read().replace('\n','').split(',') # <- why tf are these separate files? maybe there's a reason
money = ibis.read_csv('/s3/fec/itcont.txt', sep = '|', names = headers, header = False, quote="")

## load name link
with open('assets/ccl_header_file.csv') as file: headers = file.read().replace('\n','').split(',') # <- why tf are these separate files? maybe there's a reason
namelink = ibis.read_csv('assets/ccl.txt', sep = '|', names = headers, header = False)

## merge
money = (
    money
    .join(
        namelink, money['CMTE_ID'] == namelink['CMTE_ID']
    )
)

# aggregate money calculation for legislator and state
state_data = (
    money
    .group_by(['CAND_ID', 'STATE'])
    .agg(
        total = _['TRANSACTION_AMT'].sum(), 
        count = _.count()
    )
).execute()
state_data.to_csv('.tmp/state_data.csv', index = None)
state_data = pd.read_csv('.tmp/state_data.csv')

# aggregate money calculation for legislator
money_agg = (
    money
    .group_by(['CAND_ID'])
    .agg(
        total_money=_['TRANSACTION_AMT'].sum(),
        total_ind_don=_.count()
    )
).execute()


## merge with legislators
legislators['fec_ind_id'] = legislators['fec_ids'].apply(lambda x: x.split(',')[0])
legislators_w_spending = legislators.merge(money_agg, left_on = "fec_ids", right_on = "CAND_ID", how = "inner")
legislators_w_spending.to_csv('.tmp/legislators_w_spending.csv', index=False)
legislators_w_spending = pd.read_csv('.tmp/legislators_w_spending.csv')

## aggregate calculations
# print(legislators_w_spending.head())

total_money_avg_sen = round(legislators_w_spending[legislators_w_spending['type'] == 'Senator']['total_money'].mean(),2)
total_ind_don_avg_sen = legislators_w_spending[legislators_w_spending['type'] == 'Senator']['total_ind_don'].mean()

total_money_avg_rep = round(legislators_w_spending[legislators_w_spending['type'] == 'Representative']['total_money'].mean(),2)
total_ind_don_avg_rep = legislators_w_spending[legislators_w_spending['type'] == 'Representative']['total_ind_don'].mean()

print(total_ind_don_avg_rep)
print(total_ind_don_avg_sen)

# get rankings
legislators_w_spending['total_money_rank'] = legislators_w_spending['total_money'].rank(ascending = False, method = 'dense')
legislators_w_spending['total_ind_don_rank'] = legislators_w_spending['total_ind_don'].rank(ascending = False, method = 'dense')

# Loop through legislators and get fine-grained data
for _, l in legislators_w_spending.iterrows():

    # Get candidate data
    bioguide_id = l['bioguide_id']
    candidate_data = legislators_w_spending[legislators_w_spending['bioguide_id'] == bioguide_id]

    if candidate_data.empty:
        print(f"no data for {bioguide_id}; {l['first_name']} {l['last_name']}")
        # ind_money_data.append({})
    else:
        print(f'...{bioguide_id}')
        cand_id = candidate_data['CAND_ID'].iloc[0]
        leg_state = legislators[legislators['bioguide_id'] == bioguide_id]['state'].iloc[0]

        instate = state_data[
            (state_data['CAND_ID'] == cand_id) & (state_data['STATE'] == leg_state)
        ]

        outstate = state_data[
            (state_data['CAND_ID'] == cand_id) & (state_data['STATE'] != leg_state)
        ]

        # get us map
        state_map = (
            state_data[state_data['CAND_ID'] == cand_id]
            .groupby(['STATE'])
            .aggregate(
                {'total': lambda x: sum(x)}
            )
            ['total']
            .to_dict()
        )

        ind_money_data = {
            'bioguide_id': bioguide_id,
            'total_money': int(candidate_data['total_money'].iloc[0]),
            'total_money_avg': total_money_avg_rep if l['type'] == 'rep' else total_money_avg_sen,
            'total_ind_don': int(candidate_data['total_ind_don'].iloc[0]),
            'total_ind_don_avg': total_ind_don_avg_rep if l['type'] == 'rep' else total_ind_don_avg_sen,
            'instate_total': int(instate['total'].sum()),
            'instate_count': int(instate['count'].sum()),
            'outstate_total': int(outstate['total'].sum()),
            'outstate_count': int(outstate['count'].sum()),
            'total_money_rank': candidate_data['total_money_rank'].iloc[0],
            'total_ind_don_rank': candidate_data['total_ind_don_rank'].iloc[0],
            'state_map': state_map,
        }

        with dataset.connect(params) as dbx:
            dbx[tablename].upsert(
                ind_money_data,
                'bioguide_id'
            )




# idk what this was for
# # Upload to RDB
# with dataset.connect(params) as dbx:
#     dbx[tablename].upsert_many(
#         ind_money_data,
#         'bioguide_id'
#     )


