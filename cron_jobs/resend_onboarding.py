
import yaml
import os

local_path = os.environ["APP_PATH"]
with open(os.path.join(local_path, "config.yaml")) as file:
    config = yaml.load(file, Loader=yaml.FullLoader)
import sys

sys.path.append(local_path + "/src")
from onboard import onboard_template
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

to_ts = datetime.datetime.now() - datetime.timedelta(hours=0)
from_ts = datetime.datetime.now() - datetime.timedelta(days=3)

asha_onboarding_msgs = bot_conv_db.find_all_with_duration(from_ts, to_ts, 'Asha_onboarding_template')
anm_onboarding_msgs = bot_conv_db.find_all_with_duration(from_ts, to_ts, 'ANM_onboarding_template')

user_onboarding_msgs = asha_onboarding_msgs + anm_onboarding_msgs

print("asha_onboarding_msgs", len(asha_onboarding_msgs))
print("anm_onboarding_msgs", len(anm_onboarding_msgs))

count = {}

for msg in user_onboarding_msgs:
    user_row = user_db.get_from_user_id(msg['receiver_id'])

    onboarding_response_row = user_conv_db.get_from_reply_id(msg['message_id'])

    if onboarding_response_row is not None:
        print("Onboarding response found for user", user_row['user_id'])
        continue

    onboarding_response_row = user_conv_db.get_all_user_conv(user_row['user_id'], 'onboarding_response')

    if len(onboarding_response_row) > 0:
        print("Onboarding response found for user", user_row['user_id'])
        continue

    onboarding_messages = bot_conv_db.find_with_receiver_id(user_row['user_id'], msg['message_type'])

    if len(onboarding_messages) > 1:
        print("Multiple onboarding messages found for user", user_row['user_id'])
        continue

    print("Sending onboarding template to user again", user_row['user_id'])
    count[user_row['user_type']] = count.get(user_row['user_type'], 0) + 1
    onboard_template(config, app_logger, user_row, messenger, bot_conv_db)

print("Sent onboarding messages to", count)
