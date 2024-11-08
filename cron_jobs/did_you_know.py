import yaml
import os
from cachetools import TTLCache

local_path = os.environ["APP_PATH"]
with open(os.path.join(local_path, "config.yaml")) as file:
    config = yaml.load(file, Loader=yaml.FullLoader)
import sys

sys.path.append(local_path + "/src")

from knowledge_base import KnowledgeBase
from database import UserDB, UserConvDB, BotConvDB, AppLogger
from conversation_database import LoggingDatabase
from messenger.whatsapp import WhatsappMessenger
from azure_language_tools import translator
import os
import random
import datetime
import pandas as pd

# 3 days in seconds: 3 days * 24 hours/day * 60 minutes/hour * 60 seconds/minute
three_days_ttl = 3 * 24 * 60 * 60  # 259200 seconds
cache = TTLCache(ttl=three_days_ttl, maxsize=1000)

DID_YOU_KNOW = "did_you_know"
GUID = 'GUID'
FACT = 'Did you know - Hindi'
FACT_GUID_KEY = 'dyk_guids'
template_name = "did_you_know"

user_db = UserDB(config)
user_conv_db = UserConvDB(config)
bot_conv_db = BotConvDB(config)
app_logger = AppLogger()
messenger = WhatsappMessenger(config, app_logger)
azure_translate = translator()

print("Date: ", datetime.datetime.now())

# users = [user_db.get_from_whatsapp_id('918837701828')]
# print("Total users: ", len(users))


def get_next_fact(user_row, facts_df):
    fact_guids = facts_df.index.tolist()
    user_fact_guids_dict = user_row.get(FACT_GUID_KEY, [])
    user_fact_guids = user_fact_guids_dict[FACT_GUID_KEY]
    print("User fact guids: ", user_fact_guids)
    remaining_guids = list(set(fact_guids) - set(user_fact_guids))
    if remaining_guids == []:
        remaining_guids = fact_guids
        user_fact_guids = []
    next_fact_guid = random.choice(remaining_guids)
    user_fact_guids.append(next_fact_guid)
    return facts_df.loc[next_fact_guid][FACT], user_fact_guids

def send_fact(users_df, facts_df):
    for _, user_row in users_df.iterrows():

        if user_row.get("opt out", False) and not pd.isna(user_row["opt out"]):
            print("User opted out: ", user_row["whatsapp_id"], user_row["opt out"])
            continue
        fact, user_fact_guids = get_next_fact(user_row, facts_df)
        sent_msg_id = messenger.send_template(
            user_row["whatsapp_id"],
            template_name,
            user_row["user_language"],
            [fact],
            None
        )
        user_db.update_user_dyk_guids(user_row["user_id"], {"dyk_guids": user_fact_guids})

        bot_conv_db.insert_row(
            receiver_id=user_row["user_id"],
            message_type=DID_YOU_KNOW,
            message_id=sent_msg_id,
            audio_message_id=None,
            message_source_lang="en",
            message_language=user_row["user_language"],
            message_english=fact,
            reply_id=None,
            citations=None,
            message_timestamp=datetime.datetime.now(),
            transaction_message_id=None,
            did_you_know_id=user_fact_guids[-1],
        )

def get_suggested_questions_based_on_fact(
    id,
    row_lt,
    facts_df,
    knowledge_base: KnowledgeBase,
    onboarding_questions,
):
    if id in cache:
        return cache[id]
    fact = facts_df.iloc[id]["Fact"]
    source_lang = row_lt["user_language"]
    next_questions = knowledge_base.follow_up_questions(
        fact, "ignore chatbot answer", row_lt['user_type'], app_logger
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
    cache[id] = (title, list_title, questions_source)
    return title, list_title, questions_source

def send_fact_to_Asha():
    users = user_db.get_all_users(user_type="Asha")
    user_df = pd.DataFrame(users)
    facts_df = pd.read_csv(local_path + "/data/asha_bot/did_you_know/did_you_know.csv", encoding='utf-8')
    facts_df.set_index(GUID, inplace=True)
    app_logger.add_log(event_name="did_you_know", details={"message": f"Total users: {len(users)}"})
    try:
        send_fact(user_df, facts_df)
        app_logger.add_log(event_name="did_you_know", details={"message": "Successfully sent facts to Asha"})
    except Exception as e:
        app_logger.add_log(event_name="did_you_know", details={"message": f"Error in sending facts to Asha: {str(e)}"})
    
if __name__ == "__main__":
    send_fact_to_Asha()
