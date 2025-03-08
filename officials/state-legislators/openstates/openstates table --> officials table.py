import os, json, urllib.request, time, re
from datetime import datetime

import dotenv
import ibis; from ibis import _
import dataset
import numpy as np 
import pandas as pd
import json5

# # # # # # # #  
# SETUP
# # # # # # # # 
dotenv.load_dotenv('../../../env'); dotenv.load_dotenv(os.environ['PATH_TO_SECRETS'])
params = f"{os.environ['DB_DIALECT']}://{os.environ['DB_USER']}:{urllib.parse.quote(os.environ['DB_PASSWORD'])}@localhost:{os.environ['DB_PORT']}/elite"
paramsops = f"{os.environ['DB_DIALECT']}://{os.environ['DB_USER']}:{urllib.parse.quote(os.environ['DB_PASSWORD'])}@localhost:{os.environ['DB_PORT']}/ops"
db = ibis.mysql.connect(
    host=os.environ['DB_HOST'],
    user=os.environ['DB_USER'],
    password=os.environ['DB_PASSWORD'],
    database='elite',
)

# Fetch the officials_state table structure and states list
officials_state = (
    db.table('officials')
    .filter([_.level == 'state'])
    .execute()
    .replace({pd.NaT: None})
)

openstates_profiles = (
    db.table('openstates')
    .execute()
    .replace({pd.NaT: None})
)

# JOIN
result = pd.merge(
    openstates_profiles, 
    officials_state[['openstates_id']], 
    left_on='openstates_id', 
    right_on='openstates_id', 
    how='left', 
)

# CLEAN
result = result[['openstates_data','openstates_id','state']]
result.loc[:, 'active'] = 1

# def clean_openstates_json(x):
#     try: 
#         x = re.sub(r'\bTrue\b', 'true', x)
#         x = re.sub(r'\bFalse\b', 'false', x)
#         x = re.sub(r'\bNone\b', 'null', x)
#         return json5.loads(x)
#     except Exception as e: 
#         print(f'FAILED!!! with {e} \n\n|\n\njson: {x};{type(x)}')
#         return x
# result.loc[:, 'openstates_data'] = result['openstates_data'].apply(clean_openstates_json)

result.loc[:, 'name'] = result['openstates_data'].apply(lambda x: x.get('name'))
result.loc[:, 'level'] = 'state'

# result.loc[:, 'new_addition_from_openstates'] = result['openstates_id'].isnull() == False
# result.loc[:, 'openstates_data'] = result['openstates_data'].apply(lambda x: json.dumps(x))

result = result.drop(columns=["openstates_data"])
result = result.replace({np.nan: None})

# PUSH TO DATABASE
## Set all state legislators active to 0 (so we're assuming if they aren't in the openstates repo they aren't in office anymore)
db.raw_sql("""
UPDATE officials SET active = 0 WHERE level = 'state'
""")

print('UPDATE ENTRIES BASED ON OPENSTATES DATA')
dbx = dataset.connect(params)
dbx['officials'].upsert_many(
    result.to_dict(orient = 'records'),
    'openstates_id'
)
dbx.engine.dispose(); dbx.close()




