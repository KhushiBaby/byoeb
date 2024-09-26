import datetime
import sys
import yaml
import traceback
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

userdb = UserDB(config)
user_conv_db = UserConvDB(config)
bot_conv_db = BotConvDB(config)
expert_conv_db = ExpertConvDB(config)

responder = WhatsappResponder(config)

import pandas as pd
from tqdm import tqdm



to_ts = datetime.datetime.now() - datetime.timedelta(hours=0)
from_ts = datetime.datetime.now() - datetime.timedelta(days=1)

list_queries = user_conv_db.get_all_unresolved(from_ts, to_ts)


df = pd.DataFrame(list_queries)

if len(df) == 0:
    print("No unresolved queries")
    sys.exit()

    
df = df[df['query_type'] != 'small-talk']
df = df[df['message_type'] != 'feedback_response']
df = df[df['message_type'] != 'onboarding_response']
df.reset_index(drop=True, inplace=True)

category_to_expert = {}

for expert in config["EXPERTS"]:
    category_to_expert[config["EXPERTS"][expert]] = expert

for i, row in tqdm(df.iterrows()):
    try:
        user_row = userdb.get_from_user_id(row['user_id'])
        is_test_user = user_row.get("test_user", False)
        responder.escalate_query_multiple(row, is_test_user)
    except Exception as e:
        print("Error in escalation")
        print(row)
        traceback.print_exc()
    

print("Escalation done")