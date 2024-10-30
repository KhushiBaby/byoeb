import yaml
import os
import smtplib



local_path = os.environ["APP_PATH"]
with open(os.path.join(local_path, "config.yaml")) as file:
    config = yaml.load(file, Loader=yaml.FullLoader)
import sys
sys.path.append(local_path + "/processing")
sys.path.append(local_path + "/src")

from get_secrets import get_emails_list
from database import UserDB, UserConvDB, BotConvDB, ExpertConvDB, AppLogger
from messenger.whatsapp import WhatsappMessenger
from tabulate import tabulate
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import datetime
import pandas as pd
import utils
import re
import hashlib

# DB key names
MESSAGE_SOURCE_LANG = 'message_source_lang'
MESSAGE_ENGLISH = 'message_english'
MESSAGE_ID = 'message_id'
REPLY_ID = 'reply_id'
USER_ID = 'user_id'
TEST_USER = 'test_user'

# SpreadSheet column names
QUERY_SOURCE_LANG = 'Query in Source Language (Hindi/Hinglish)'
QUERY_ENG = 'Query in English for Knowledge Base'
RESPONSE = 'GPT Answer/Final Answer for Knowledge Base'
ADD_TO_KB = 'Add to Knowledge Base (Yes/No)'
RELEVANT_DOC = 'Relevant document (if needed)'

NEW_RANGE_NAME = 'KB_Update_' + datetime.datetime.now().strftime("%d-%m-%Y")
OLD_RANGE_NAME = 'KB_Update_' + (datetime.datetime.now() - datetime.timedelta(days=7)).strftime("%d-%m-%Y")
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
SPREADSHEET_ID = config["SPREADSHEET_ID"].strip()

HOURS_TO_SKIP = 2
DAYS_TO_LOOKBACK = 7


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

def try_get_older_sheet_name(local_path):
    sheet_names = utils.get_sheet_names(SCOPES, SPREADSHEET_ID, local_path)
    latest_entry = utils.get_latest_entry(sheet_names)
    if latest_entry is None:
        return None
    if latest_entry == NEW_RANGE_NAME:
        return None
    print(f"Found older range name: {latest_entry}")
    return latest_entry

def get_unanswered_questions_from_last_update(old_range_name, local_path):
    if not utils.is_sheet_present(SCOPES, SPREADSHEET_ID, old_range_name, local_path):
        print("Looking for older sheet")
        old_range_name = try_get_older_sheet_name(local_path)
        if old_range_name is None:
            print("No older sheet found")
            return None, None
        
    data = utils.pull_sheet_data(SCOPES, SPREADSHEET_ID, old_range_name, local_path)
    df_previous = utils.convert_to_dataframe(data)
    df_unanswered = df_previous[(df_previous[ADD_TO_KB].str.strip().str.upper() != 'YES') & (df_previous[ADD_TO_KB].str.strip().str.upper() != 'NO')]
    return df_unanswered, old_range_name

def get_idk_questions():
    old_range_name = OLD_RANGE_NAME
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

    end_dt = datetime.datetime.now() - datetime.timedelta(hours=HOURS_TO_SKIP)
    start_dt = end_dt - datetime.timedelta(days=DAYS_TO_LOOKBACK)

    user_conv_queries = user_conv_db.get_all_queries_in_duration(start_dt, end_dt)
    user_conv_df = pd.DataFrame(user_conv_queries)
    user_conv_df = user_conv_df[user_conv_df['query_type'] == 'Clinical']

    bot_conv_queries = bot_conv_db.find_all_with_duration(start_dt, end_dt + datetime.timedelta(hours=HOURS_TO_SKIP))
    bot_conv_df = pd.DataFrame(bot_conv_queries)

    questions_with_idks = pd.DataFrame(columns=[QUERY_SOURCE_LANG, QUERY_ENG, RESPONSE, ADD_TO_KB, RELEVANT_DOC])
    previous_unanswered_df, old_range_name = get_unanswered_questions_from_last_update(old_range_name, local_path)
    
    if previous_unanswered_df is not None:
        print("unanswered questions from last update: ", len(previous_unanswered_df)) 
        for _, row in previous_unanswered_df.iterrows():
            question_set.add(md5_hash(row[QUERY_ENG]))
    questions_with_idks = pd.concat(
        [
            questions_with_idks,
            previous_unanswered_df
        ],
        ignore_index=True
    )
    test_users = user_db.get_test_users()
    test_users_ids = [user[USER_ID] for user in test_users]
    for _, row in user_conv_df.iterrows():
        user_id = row[USER_ID]
        if user_id in test_users_ids:
            print("Skipping test user")
            continue
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
                        ADD_TO_KB: '',
                        RELEVANT_DOC: ""
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
    return questions_with_idks, old_range_name

def send_email():
    email_id = os.environ["EMAIL_ID"].strip()
    email_pass = os.environ["EMAIL_PASS"].strip()
    li = get_emails_list()
    # li = config["EMAIL_LIST"]
    link_to_sheet = config["SHEET_LINK"].strip()
    date_today = datetime.datetime.now()
    for dest in li:
        # Create the email message
        msg = MIMEMultipart()
        msg['From'] = config["EMAIL_ID"]
        msg['To'] = dest
        msg['Subject'] = f"ASHABot Knowledge Base Update for {date_today.strftime('%d-%m-%Y')}"

        # Create the HTML message body
        message = f"""
        <html>
            <body>
                <p>Hi Dr. Rohini and Dr. Ruchit,</p>
                <p>Here is the <a href="{link_to_sheet}">Knowledge base update sheet</a>.</p>
                <p>For each row, please mention YES/NO in the <i>"Add to Knowledge Base"</i> column.<br>
                If YES, please edit <i>"Query in English for Knowledge Base"</i> and <i>"GPT Answer/Final Answer for Knowledge Base"</i> if needed.<br>
                You can link other resources in <i>"Relevant document (if needed)."</i></p>
                <p>We will add the rows marked "YES" to ASHABot's knowledge base on {(date_today + datetime.timedelta(days=3)).strftime('%d-%m-%Y')}, at 10PM PST.</p>
                <p>Best regards,<br>ASHABot team.</p>
            </body>
        </html>
        """

        # Attach the HTML message to the email
        msg.attach(MIMEText(message, 'html'))

        # Send the email
        with smtplib.SMTP("smtp.gmail.com", 587) as s:
            s.starttls()
            s.login(email_id, email_pass)
            s.sendmail(email_id, dest, msg.as_string())

        print(f"Email sent to: {dest}")

questions_with_idks, old_range_name = get_idk_questions()
if utils.is_sheet_present(SCOPES, SPREADSHEET_ID, NEW_RANGE_NAME, local_path):
    utils.delete_sheet(SCOPES, SPREADSHEET_ID, NEW_RANGE_NAME, local_path)
utils.create_sheet(SCOPES, SPREADSHEET_ID, NEW_RANGE_NAME, local_path)
utils.add_headers(SCOPES, SPREADSHEET_ID, NEW_RANGE_NAME, [QUERY_SOURCE_LANG, QUERY_ENG, RESPONSE, ADD_TO_KB, RELEVANT_DOC], local_path)
utils.append_rows(SCOPES, SPREADSHEET_ID, NEW_RANGE_NAME, questions_with_idks, local_path)
utils.set_row_bold(SCOPES, SPREADSHEET_ID, NEW_RANGE_NAME, 1, local_path)
send_email()
if old_range_name is not None and utils.is_sheet_present(SCOPES, SPREADSHEET_ID, old_range_name, local_path):
    utils.delete_sheet(SCOPES, SPREADSHEET_ID, old_range_name, local_path)