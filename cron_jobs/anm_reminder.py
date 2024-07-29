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



to_ts = datetime.datetime.now() - datetime.timedelta(hours=0)
from_ts = datetime.datetime.now() - datetime.timedelta(days=3)

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
    previous_poll_requests = bot_conv_db.find_all_with_transaction_id(row["message_id"], "response_request")
    previous_polls = bot_conv_db.find_all_with_transaction_id(row["message_id"], "consensus_poll")
    previous_consensus_responses = expert_conv_db.get_from_transaction_message_id(row["message_id"], "consensus_response")

    response_sent = {}
    experts_processed = set()
    for response in previous_consensus_responses:
        response_sent[response["user_id"]] = True


    for poll in previous_polls:
        experts_processed.add(poll["receiver_id"])
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

    #if request is ignored, send again

    for poll_request in previous_poll_requests:
        if poll_request["receiver_id"] in experts_processed:
            continue
        experts_processed.add(poll_request["receiver_id"])
        current_time = datetime.datetime.now()
        delta = current_time - poll_request["message_timestamp"]
        if (poll_request["receiver_id"] in response_sent) or (delta.total_seconds() / 3600) < 2:
            continue

        requests = bot_conv_db.find_with_transaction_id_and_receiver_id(poll_request["transaction_message_id"], poll_request["receiver_id"], "response_request")
        if len(requests) > 1:
            print("More than one request found")
            continue

        print("Sending the request again to expert")
        # responder.send_reminder_anm(poll_request, template=True)
        expert_row_lt = userdb.get_from_user_id(poll_request["receiver_id"])
        responder.send_query_request_expert(expert_row_lt, row)