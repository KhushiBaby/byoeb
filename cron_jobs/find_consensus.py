import datetime
import sys
import yaml
import os

local_path = os.environ["APP_PATH"]
with open(local_path + "/config.yaml") as file:
    config = yaml.load(file, Loader=yaml.FullLoader)

sys.path.append(local_path.strip() + "/src")


from database import UserConvDB

from responder import WhatsappResponder

user_conv_db = UserConvDB(config)

responder = WhatsappResponder(config)

import pandas as pd
from tqdm import tqdm



to_ts = datetime.datetime.now() - datetime.timedelta(hours=0)
from_ts = datetime.datetime.now() - datetime.timedelta(days=7)

list_cursor = user_conv_db.get_all_unresolved(from_ts, to_ts)

df = pd.DataFrame(list(list_cursor))

# print(consensus_prompt)

for index, row in tqdm(df.iterrows()):

    if row['query_type'] != 'small-talk':
        responder.find_consensus(row)





