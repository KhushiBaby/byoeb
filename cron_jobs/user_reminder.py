
import yaml
import os

local_path = os.environ["APP_PATH"]
with open(os.path.join(local_path, "config.yaml")) as file:
    config = yaml.load(file, Loader=yaml.FullLoader)
import sys

sys.path.append(local_path + "/src")

from database import UserDB, UserConvDB, BotConvDB, AppLogger
from conversation_database import LoggingDatabase
from messenger.whatsapp import WhatsappMessenger
import os
import json
import datetime
import pandas as pd

template_name = "asha_reminder"

user_db = UserDB(config)
user_conv_db = UserConvDB(config)
bot_conv_db = BotConvDB(config)
app_logger = AppLogger()
messenger = WhatsappMessenger(config, app_logger)

print("Date: ", datetime.datetime.now())

users = user_db.get_all_users(user_type="Asha")
print("Total users: ", len(users))
df = pd.DataFrame(users)


for i, user_row in df.iterrows():

    if user_row.get("opt out", False) and not pd.isna(user_row["opt out"]):
        print("User opted out: ", user_row["whatsapp_id"], user_row["opt out"])
        continue

    # check last reminder message, if less than 7 days old, continue
    last_reminder = bot_conv_db.find_most_recent_with_receiver_id(user_row["user_id"], message_type='asha_reminder')
    print("Last reminder: ", last_reminder)
    if last_reminder is not None and (datetime.datetime.now() - last_reminder["message_timestamp"]) < datetime.timedelta(days=7):
        print("Not sending message to ", user_row["whatsapp_id"])
        continue


    try:
        last_msg = user_conv_db.get_most_recent_message(user_row["user_id"])
        last_msg_type = last_msg.get("message_type", 'small-talk')
        day_delta = 2 if last_msg_type == 'onboarding_response' else 7
        if last_msg is None or ((datetime.datetime.now() - last_msg["message_timestamp"]) > datetime.timedelta(days=day_delta)):
            print("Sending message to ", user_row["whatsapp_id"])
            sent_msg_id = messenger.send_template(user_row["whatsapp_id"], template_name, user_row["user_language"], None)
            bot_conv_db.insert_row(
                receiver_id=user_row['user_id'],
                message_type=f'asha_reminder',
                message_id=sent_msg_id,
                audio_message_id=None,
                message_source_lang=None,
                message_language=user_row["user_language"],
                message_english=None,
                reply_id=None,
                citations=None,
                message_timestamp=datetime.datetime.now(),
                transaction_message_id=None,
            )
        else:
            print("Not sending message to ", user_row["whatsapp_id"])
    except Exception as e:
        print("Error: ", e)
        continue