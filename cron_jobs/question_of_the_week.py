import yaml
import os

local_path = os.environ["APP_PATH"]
with open(os.path.join(local_path, "config.yaml")) as file:
    config = yaml.load(file, Loader=yaml.FullLoader)
import sys

sys.path.append(local_path + "/src")

from knowledge_base import KnowledgeBase
from database import UserDB, UserConvDB, BotConvDB, AppLogger
from conversation_database import LoggingDatabase
from messenger.whatsapp import WhatsappMessenger
from cachetools import TTLCache
import os
import json
import random
import datetime
import pandas as pd

QUESTION_OF_THE_WEEK = "question_of_the_week"
GUID = "GUID"
QUESTION = "Question"
QUESTION_GUID_KEY = "qow_guids"
ANSWER = "Correct Answer"
EVENT_NAME = "question_of_the_week"

template_name = "question_of_the_week"

user_db = UserDB(config)
user_conv_db = UserConvDB(config)
bot_conv_db = BotConvDB(config)
app_logger = AppLogger()
messenger = WhatsappMessenger(config, app_logger)

print("Date: ", datetime.datetime.now())

# 3 days in seconds: 3 days * 24 hours/day * 60 minutes/hour * 60 seconds/minute
three_days_ttl = 3 * 24 * 60 * 60  # 259200 seconds
q_n_a_df = pd.read_csv(local_path + "/data/asha_bot/question_of_the_week/q_n_a.csv")
questions_df = q_n_a_df[[GUID, QUESTION]]
questions_df.set_index(GUID, inplace=True)
answer_df = q_n_a_df[[GUID, ANSWER]]
answer_df.set_index(GUID, inplace=True)
cache = TTLCache(ttl=three_days_ttl, maxsize=1000)

def get_next_question(user_row):
    qow_guids = questions_df.index.tolist()
    user_qow_guids_dict = None
    if pd.isna(user_row.get(QUESTION_GUID_KEY)) or user_row[QUESTION_GUID_KEY] is None:
        user_qow_guids_dict = {QUESTION_GUID_KEY: []}
    else:
        user_qow_guids_dict = user_row.get(QUESTION_GUID_KEY)
    user_qow_guids = user_qow_guids_dict[QUESTION_GUID_KEY]
    print("User QoW guids: ", user_qow_guids)
    remaining_guids = list(set(qow_guids) - set(user_qow_guids))
    if remaining_guids == []:
        remaining_guids = qow_guids
        user_qow_guids = []
    next_qow_guid = random.choice(remaining_guids)
    user_qow_guids.append(next_qow_guid)
    return questions_df.loc[next_qow_guid][QUESTION], user_qow_guids

def send_question(user_df):
    for i, user_row in user_df.iterrows():

        if user_row.get("opt out", False) and not pd.isna(user_row["opt out"]):
            print("User opted out: ", user_row["whatsapp_id"], user_row["opt out"])
            continue
        try:
            qow, user_qow_guids = get_next_question(user_row)
            sent_msg_id = messenger.send_template(
                user_row["whatsapp_id"],
                template_name,
                user_row["user_language"],
                [qow],
                None,
            )
            user_db.update_user_qow_guids(user_row["user_id"], {QUESTION_GUID_KEY: user_qow_guids})
            bot_conv_db.insert_row(
                receiver_id=user_row["user_id"],
                message_type=QUESTION_OF_THE_WEEK,
                message_id=sent_msg_id,
                audio_message_id=None,
                message_source_lang=None,
                message_language=user_row["user_language"],
                message_english=None,
                reply_id=None,
                citations=None,
                message_timestamp=datetime.datetime.now(),
                transaction_message_id=None,
                question_id=user_qow_guids[-1],
            )
        except Exception as e:
            print("Error: ", e)
            app_logger.add_log(
                event_name=EVENT_NAME,
                details={"message": f"Error sending QoW to user: {user_row['user_id']}: {str(e)}"},
            )


def get_answer(guid):
    return answer_df.loc[guid]["Correct Answer"]

def get_suggested_questions(
    guid,
    row_lt,
    knowledge_base: KnowledgeBase,
    onboarding_questions,
    azure_translate
):
    if guid in cache:
        return cache[guid]
    source_lang = row_lt["user_language"]
    ques = questions_df.loc[guid]["Question"]
    ques = azure_translate.translate_text(ques, "hi", "en", app_logger)
    ans = answer_df.loc[guid]["Correct Answer"]
    next_questions = knowledge_base.follow_up_questions(
        ques, ans, row_lt['user_type'], app_logger
    )
    questions_source = []
    for question in next_questions:
        question_source = azure_translate.translate_text(
            question, "en", source_lang, app_logger
        )
        questions_source.append(question_source)
    title, list_title = (
        onboarding_questions[source_lang]["title"],
        onboarding_questions[source_lang]["list_title"],
    )
    cache[guid] = (title, list_title, questions_source)
    return title, list_title, questions_source

def send_question_of_week_to_Asha():
    users = user_db.get_all_users(user_type="Asha")
    user_df = pd.DataFrame(users)
    try:
        send_question(user_df)
        app_logger.add_log(event_name=EVENT_NAME, details={"message": "Successfully sent QoW to Asha"})
    except Exception as e:
        print("Error in sending QoW to Asha: ", str(e))
        app_logger.add_log(event_name=EVENT_NAME, details={"message": f"Error in sending QoW to Asha: {str(e)}"})

if __name__ == "__main__":
    send_question_of_week_to_Asha()
