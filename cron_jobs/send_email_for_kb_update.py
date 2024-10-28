import yaml
import os
import smtplib

local_path = os.environ["APP_PATH"]
with open(os.path.join(local_path, "config.yaml")) as file:
    config = yaml.load(file, Loader=yaml.FullLoader)
import sys
sys.path.append(local_path + "/src")

from database import UserDB, UserConvDB, BotConvDB, ExpertConvDB, AppLogger
from messenger.whatsapp import WhatsappMessenger
from tabulate import tabulate
import datetime
import pandas as pd
import utils
import hashlib

# DB key names
MESSAGE_SOURCE_LANG = 'message_source_lang'
MESSAGE_ENGLISH = 'message_english'
MESSAGE_ID = 'message_id'
REPLY_ID = 'reply_id'

# SpreadSheet column names
QUERY_SOURCE_LANG = 'Query in Source Language'
QUERY_ENG = 'Query in English'
RESPONSE = 'GPT Answer/Final Answer for Knowledge Base'
ADD_TO_KB = 'Add to Knowledge Base (Yes/No)'
RELEVANT_DOC = 'Relevant document (if needed)'

NEW_RANGE_NAME = 'KB_Update_' + datetime.datetime.now().strftime("%d-%m-%Y")
OLD_RANGE_NAME = 'KB_Update_' + (datetime.datetime.now() - datetime.timedelta(days=7)).strftime("%d-%m-%Y")
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
SPREADSHEET_ID = '1OBCVwvKC6xBl-FEfCNnJa8C36w8hlT49iGXzYya7s3s'


def md5_hash(input_string: str) -> str:
    # Create an MD5 hash object
    hash_object = hashlib.md5(input_string.encode())

    # Return the hash as a hexadecimal string
    return hash_object.hexdigest()

def get_prompt(query):
    system_prompt = "You are Asha bot. Your purpose is to help Asha workers with any queries that they might have while doing their Asha duties. While reading the query, please keep in mind that the query is written by Asha workers and may contain spelling mistakes or grammatical errors. As Community Health Workers are low literate, please ignore their grammatical and spelling errors, and try to make sense of their query asked in Hinglish (a combination of Hindi and English). You MUST respond in english only"

    prompt = [{"role": "system", "content": system_prompt}]
    prompt.append({"role": "user", "content": query})
    return prompt

def get_unanswered_questions_from_previous(local_path):
    if not utils.is_sheet_present(SCOPES, SPREADSHEET_ID, OLD_RANGE_NAME, local_path):
        return None
    data = utils.pull_sheet_data(SCOPES, SPREADSHEET_ID, OLD_RANGE_NAME, local_path)
    df_previous = pd.DataFrame(data[1:], columns=data[0])
    df_unanswered = df_previous[(df_previous[ADD_TO_KB].isnull()) | (df_previous[ADD_TO_KB].str.strip().str.upper() == 'NA')]
    return df_unanswered

question_set = set()

phrases_to_check = [
    "I'm sorry for the inconvenience",
    "I'm sorry, but as a chatbot",
    "I do not know the answer",
    "Unfortunately, as a chatbot",
    "I'm sorry, but your"
]

user_db = UserDB(config)
user_conv_db = UserConvDB(config)
bot_conv_db = BotConvDB(config)
expert_conv_db = ExpertConvDB(config)
logger = AppLogger()

HOURS_TO_SKIP = 2
DAYS_TO_LOOKBACK = 7

end_dt = datetime.datetime.now() - datetime.timedelta(hours=HOURS_TO_SKIP)
start_dt = end_dt - datetime.timedelta(days=DAYS_TO_LOOKBACK)

user_conv_queries = user_conv_db.get_all_queries_in_duration(start_dt, end_dt)
user_conv_df = pd.DataFrame(user_conv_queries)
user_conv_df = user_conv_df[user_conv_df['query_type'] == 'Clinical']

bot_conv_queries = bot_conv_db.find_all_with_duration(start_dt, end_dt + datetime.timedelta(hours=HOURS_TO_SKIP))
bot_conv_df = pd.DataFrame(bot_conv_queries)

questions_with_idks = pd.DataFrame(columns=[QUERY_SOURCE_LANG, QUERY_ENG, RESPONSE, ADD_TO_KB, RELEVANT_DOC])
previous_unanswered_df = get_unanswered_questions_from_previous(local_path)
questions_with_idks = pd.concat(
    [
        questions_with_idks,
        previous_unanswered_df
    ],
    ignore_index=True
)

# print(len(questions_with_idks))
for index, row in user_conv_df.iterrows():
    query_source_lang = row[MESSAGE_SOURCE_LANG]
    query_eng = row[MESSAGE_ENGLISH]
    bot_answer = bot_conv_df[bot_conv_df[REPLY_ID] == row[MESSAGE_ID]].iloc[0][MESSAGE_ENGLISH]
    if any(phrase in bot_answer for phrase in phrases_to_check) and md5_hash(query_eng) not in question_set:
        question_set.add(md5_hash(query_eng))
        gpt_response = utils.get_llm_response(get_prompt(query_eng))
        new_entry_df = pd.DataFrame(
            [
                {
                    QUERY_SOURCE_LANG: query_source_lang, 
                    QUERY_ENG: query_eng, 
                    RESPONSE: gpt_response,
                    ADD_TO_KB: 'NA',
                    RELEVANT_DOC: 'NA'
                }
            ]
        )
        questions_with_idks = pd.concat(
            [
                questions_with_idks,
                new_entry_df
            ],
            ignore_index=True
        )
questions_with_idks.reset_index(drop=True, inplace=True)
if utils.is_sheet_present(SCOPES, SPREADSHEET_ID, NEW_RANGE_NAME, local_path):
    utils.delete_all_rows(SCOPES, SPREADSHEET_ID, NEW_RANGE_NAME, local_path)
else:
    utils.create_sheet(SCOPES, SPREADSHEET_ID, NEW_RANGE_NAME, local_path)
utils.add_rows(SCOPES, SPREADSHEET_ID, NEW_RANGE_NAME, questions_with_idks, local_path)
print(len(questions_with_idks))

li = config["EMAIL_LIST"]
link_to_sheet = config["SHEET_LINK"].strip()
date_today = datetime.datetime.now().strftime("%d-%m-%Y")
for dest in li:
    s = smtplib.SMTP("smtp.gmail.com", 587)
    s.starttls()
    s.login(config["EMAIL_ID"], config["EMAIL_PASS"].strip())
    message = f"Subject: BYOeB log {date_today}. \n\nHello team, \nHere is a link to today's BYOeB log: {link_to_sheet}. \n\nPlease update the column 'To Update Knowledge Base' with a YES/NO depending upon the expert's correction. \n\n\
Best regards, \BYOeB Bot team."
    s.sendmail(config["EMAIL_ID"], dest, message)
    print(dest, li)
    s.quit()

utils.delete_sheet(SCOPES, SPREADSHEET_ID, OLD_RANGE_NAME, local_path)