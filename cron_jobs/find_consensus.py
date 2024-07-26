import datetime
import sys
import yaml
import json
import os

local_path = os.environ["APP_PATH"]
with open(local_path + "/config.yaml") as file:
    config = yaml.load(file, Loader=yaml.FullLoader)

sys.path.append(local_path.strip() + "/src")


from database import UserDB, UserConvDB, BotConvDB, ExpertConvDB, UserRelationDB


from messenger import WhatsappMessenger
from responder import WhatsappResponder
from conversation_database import (
    LoggingDatabase
)

import subprocess
from utils import get_llm_response, remove_extra_voice_files

MIN_CONSENSUS_RESPONSES = 3

userdb = UserDB(config)
user_conv_db = UserConvDB(config)
bot_conv_db = BotConvDB(config)
expert_conv_db = ExpertConvDB(config)

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





