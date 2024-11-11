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
from tqdm import tqdm

QUESTION_OF_THE_WEEK = "question_of_the_week"
GUID = "GUID"
QUESTION = "Question-Hindi"
QUESTION_GUID_KEY = "qow_guids"
ANSWER = "Answer-Hindi"
EVENT_NAME = "question_of_the_week"
SEND = "Send"
EVENT_TYPE_SHOW = "show"

template_name = "question_of_the_week"

user_db = UserDB(config)
user_conv_db = UserConvDB(config)
bot_conv_db = BotConvDB(config)
app_logger = AppLogger()
messenger = WhatsappMessenger(config, app_logger)

print("Date: ", datetime.datetime.now())

# 3 days in seconds: 3 days * 24 hours/day * 60 minutes/hour * 60 seconds/minute
three_days_ttl = 3 * 24 * 60 * 60  # 259200 seconds
q_n_a_df = pd.read_csv(local_path + "/data/asha_bot/question_of_the_week/qna_v1.csv")
questions_df = q_n_a_df[[GUID, QUESTION]]
questions_df.set_index(GUID, inplace=True)
answer_df = q_n_a_df[[GUID, ANSWER]]
answer_df.set_index(GUID, inplace=True)
cache = TTLCache(ttl=three_days_ttl, maxsize=1000)

def reset_all_users_qow():
    users = user_db.get_all_users(user_type="Asha")
    user_df = pd.DataFrame(users)
    for _, user_row in tqdm(user_df.iterrows()):
        user_db.update_user(user_row["user_id"], {QUESTION_GUID_KEY: []})

def get_next_question(user_df):
    qow_guids = questions_df.index.tolist()
    overall_qow_guids = []
    for _, user_row in user_df.iterrows():
        user_qow_guids = user_row.get(QUESTION_GUID_KEY, [])
        if not isinstance(user_qow_guids, list):
            user_qow_guids = []
        if set(user_qow_guids) == set(qow_guids):
            continue
        overall_qow_guids = list(set(overall_qow_guids + user_qow_guids))
    remaining_guids = list(set(qow_guids) - set(overall_qow_guids))
    next_qow_guid = random.choice(remaining_guids)
    return next_qow_guid

def update_user_qow_guids(qow_guid, user_row):
    user_qow_guids = user_row.get(QUESTION_GUID_KEY, [])
    if not isinstance(user_qow_guids, list):
        user_qow_guids = []
    if qow_guid in user_qow_guids:
        user_qow_guids = []
    user_qow_guids.append(qow_guid)
    print(user_qow_guids)
    user_db.update_user(user_row["user_id"], {QUESTION_GUID_KEY: user_qow_guids})

def send_question(user_df):
    next_qow_guid = get_next_question(user_df)
    print("Next QoW GUID: ", next_qow_guid)
    qow = questions_df.loc[next_qow_guid][QUESTION]
    print("Next QoW: ", qow)

    for _, user_row in user_df.iterrows():

        if user_row.get("opt out", False) and not pd.isna(user_row["opt out"]):
            print("User opted out: ", user_row["whatsapp_id"], user_row["opt out"])
            continue

        try:
            update_user_qow_guids(next_qow_guid, user_row)
            start_time = datetime.datetime.now()
            sent_msg_id = messenger.send_template(
                user_row["whatsapp_id"],
                template_name,
                user_row["user_language"],
                [qow],
                None,
            )
            end_time = datetime.datetime.now()
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
                question_id=next_qow_guid,
            )
            app_logger.add_log(
                event_name=EVENT_NAME,
                details={
                    "user_id": user_row["user_id"],
                    "event_type": SEND,
                    "question_id": next_qow_guid,
                    "question": qow,
                    "sent_time": start_time.strftime("%Y-%m-%d %H:%M:%S"),
                    "latency_to_accept": (end_time - start_time).total_seconds(),
                    "message": "Success"
                }
            )
        except Exception as e:
            print("Error: ", e)
            app_logger.add_log(
                event_name=EVENT_NAME,
                details={
                    "user_id": user_row["user_id"],
                    "event_type": SEND,
                    "question_id": next_qow_guid,
                    "question": qow,
                    "message": f"Error: {str(e)}"
                }
            )

def try_send_answer(user_row, guid, reply_id):
    answer =answer_df.loc[guid][ANSWER]
    try:
        start_time = datetime.datetime.now()
        messenger.send_message(user_row["whatsapp_id"], answer, reply_id)
        end_time = datetime.datetime.now()
        app_logger.add_log(
            event_name=EVENT_NAME,
            details={
                "user_id": user_row["user_id"],
                "event_type": EVENT_TYPE_SHOW,
                "question_id": guid,
                "question": questions_df.loc[guid][QUESTION],
                "answer": answer,
                "send_time": start_time.strftime("%Y-%m-%d %H:%M:%S"),
                "latency_to_accept": (end_time - start_time).total_seconds(),
                "message": "Success"
            }
        )
    except Exception as e:
        print("Error in sending answer: ", str(e))
        app_logger.add_log(
            event_name=EVENT_NAME,
            details={
                "user_id": user_row["user_id"],
                "event_type": EVENT_TYPE_SHOW,
                "question_id": guid,
                "question": questions_df.loc[guid][QUESTION],
                "answer": answer,
                "message": f"Error: {str(e)}"
            }
        )
        raise e

def get_answer(user_row, guid):
    app_logger.add_log(
        event_name=EVENT_NAME,
        details={
            "user_id": user_row["user_id"],
            "event_type": EVENT_TYPE_SHOW,
            "question_id": guid,
            "question": questions_df.loc[guid]["Question"],
            "answer": answer_df.loc[guid]["Correct Answer"]
        }
    )
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
    ques = questions_df.loc[guid][QUESTION]
    ques = azure_translate.translate_text(ques, source_lang, "en", app_logger)
    ans = answer_df.loc[guid][ANSWER]
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
