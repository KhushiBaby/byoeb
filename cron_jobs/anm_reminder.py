import datetime
import sys
import yaml

import os

local_path = os.environ["APP_PATH"]
with open(local_path + "/config.yaml") as file:
    config = yaml.load(file, Loader=yaml.FullLoader)

sys.path.append(local_path.strip() + "/src")

NUM_EXPERTS = 1
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

logger = LoggingDatabase(config)


to_ts = datetime.datetime.now() - datetime.timedelta(hours=0)
from_ts = datetime.datetime.now() - datetime.timedelta(days=1)

list_queries = user_conv_db.get_all_unresolved(from_ts, to_ts)


df = pd.DataFrame(list_queries)

if len(df) == 0:
    print("No unresolved queries")
    sys.exit()

    
df = df[(df['message_type'] != 'feedback_response')]
df.reset_index(drop=True, inplace=True)

category_to_expert = {}

for expert in config["EXPERTS"]:
    category_to_expert[config["EXPERTS"][expert]] = expert

for i, row in tqdm(df.iterrows()):

    print(row)
    previous_polls = bot_conv_db.find_all_with_transaction_id(row["message_id"], "consensus_poll")
    previous_consensus_responses = expert_conv_db.get_from_transaction_message_id(row["message_id"], "consensus_response")

    print("Previous poll responses", previous_polls)
    print("Previous consensus responses", previous_consensus_responses)

    if len(previous_polls) == 0:
        print("No previous polls")
        continue

    response_sent = {}
    for response in previous_consensus_responses:
        response_sent[response["user_id"]] = True

    for poll in previous_polls:
        current_time = datetime.datetime.now()
        delta = current_time - poll["message_timestamp"]
        if (poll["receiver_id"] in response_sent) or (delta.total_seconds() / 3600) < 1:
            continue

        reminders = bot_conv_db.find_all_with_transaction_id(poll["transaction_message_id"], "reminder_anm")
        if len(reminders) > 1:
            print("More than one reminder found")
            continue

        print("Sending reminder to expert")
        responder.send_reminder_anm(poll)