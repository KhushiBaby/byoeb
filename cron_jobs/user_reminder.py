
import yaml
import os

local_path = os.environ["APP_PATH"]
with open(os.path.join(local_path, "config.yaml")) as file:
    config = yaml.load(file, Loader=yaml.FullLoader)
import sys

sys.path.append(local_path + "/src")

from database import UserDB, UserConvDB, AppLogger
from conversation_database import LoggingDatabase
from messenger.whatsapp import WhatsappMessenger
import os
import json
import datetime
import pandas as pd

template_name = "asha_reminder"

user_db = UserDB(config)
user_conv_db = UserConvDB(config)
app_logger = LoggingDatabase(config)
messenger = WhatsappMessenger(config, app_logger)

print("Date: ", datetime.datetime.now())

users = user_db.get_all_users(user_type="Asha")
print("Total users: ", len(users))
df = pd.DataFrame(users)

for i, user_row in df.iterrows():

    if user_row.get("opt out", False) and not pd.isna(user_row["opt out"]):
        print("User opted out: ", user_row["whatsapp_id"], user_row["opt out"])
        continue

    try:
        last_query = user_conv_db.get_most_recent_query(user_row["user_id"])
        if last_query is None or ((datetime.datetime.now() - last_query["message_timestamp"]) > datetime.timedelta(days=7)):
            print("Sending message to ", user_row["whatsapp_id"])
            messenger.send_template(user_row["whatsapp_id"], template_name, user_row["user_language"], None)
        else:
            print("Not sending message to ", user_row["whatsapp_id"])
    except Exception as e:
        print("Error: ", e)
        continue