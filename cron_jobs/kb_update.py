import smtplib
import psutil
import yaml
import datetime

import os

print("Code started running")
local_path = os.environ["APP_PATH"]
import sys

sys.path.append(local_path + "/src")
from knowledge_base import KnowledgeBase
from conversation_database import LoggingDatabase
import pandas as pd
import utils
from tqdm import tqdm
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

with open(os.path.join(local_path, "config.yaml")) as file:
    config = yaml.load(file, Loader=yaml.FullLoader)

logger = LoggingDatabase(config)

STORE_RANGE_NAME = "KB_Update_Processed"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
SPREADSHEET_ID = config["SPREADSHEET_ID"].strip()
LAST_UPDATE_RANGE_NAME = 'KB_Update_' + (datetime.datetime.now() - datetime.timedelta(days=4)).strftime("%d-%m-%Y")
KB_UPDATE_FOLDER_NAME = "kb_update_raw"
RAW_DOCUMENTS_FOLDER_NAME = "raw_documents"

# SpreadSheet column names
QUERY_SOURCE_LANG = 'Query in Source Language (Hindi/Hinglish)'
QUERY_ENG = 'Query in English for Knowledge Base'
RESPONSE = 'GPT Answer/Final Answer for Knowledge Base'
ADD_TO_KB = 'Add to Knowledge Base (Yes/No)'
RELEVANT_DOC = 'Relevant document (if needed)'
UPDATE_REQUEST_DATE = 'Update Request Date'
UPDATED_DATE = 'Updated Date'

def send_email(process_message):
    li = config["EMAIL_LIST"]
    link_to_sheet = config["SHEET_LINK"].strip()
    date_today = datetime.datetime.now()
    for dest in li:
        # Create the email message
        msg = MIMEMultipart()
        msg['From'] = config["EMAIL_ID"]
        msg['To'] = dest
        msg['Subject'] = f"ASHABot Knowledge Base Status  on {date_today.strftime('%d-%m-%Y')}"

        # Create the HTML message body
        message = f"""
        <html>
            <body>
                <p>Hi,</p>
                <p>Here is the <a href="{link_to_sheet}">Knowledge base update sheet</a>.</p>
                <p>{process_message}<br>
                <p>Best regards,<br>ASHABot team.</p>
            </body>
        </html>
        """

        # Attach the HTML message to the email
        msg.attach(MIMEText(message, 'html'))

        # Send the email
        with smtplib.SMTP("smtp.gmail.com", 587) as s:
            s.starttls()
            s.login(config["EMAIL_ID"], config["EMAIL_PASS"].strip())
            s.sendmail(config["EMAIL_ID"], dest, msg.as_string())

        print(f"Email sent to: {dest}")

def try_get_older_sheet_name(local_path):
    sheet_names = utils.get_sheet_names(SCOPES, SPREADSHEET_ID, local_path)
    latest_entry = utils.get_latest_entry(sheet_names)
    if latest_entry is None:
        return None
    print(f"Found: {latest_entry}")
    return latest_entry

def get_answered_questions_from_last_update(last_update_range_name, local_path):
    if not utils.is_sheet_present(SCOPES, SPREADSHEET_ID, last_update_range_name, local_path):
        print("Looking for older sheet")
        last_update_range_name = try_get_older_sheet_name(local_path)
        if last_update_range_name is None:
            print("No older sheet found")
            return None, None, None
        
    data = utils.pull_sheet_data(SCOPES, SPREADSHEET_ID, last_update_range_name, local_path)
    df_last = utils.convert_to_dataframe(data)
    df_yes_to_update = df_last[(df_last[ADD_TO_KB].str.strip().str.upper() == 'YES')]
    df_no_to_update = df_last[(df_last[ADD_TO_KB].str.strip().str.upper() == 'NO')]
    return df_yes_to_update, df_no_to_update, last_update_range_name

def create_kb_update_file(df_yes_to_update, local_path):
    if df_yes_to_update is None or df_yes_to_update.empty:
        print("No new update")
        return False
    df_yes_to_update.reset_index(drop=True, inplace=True)
    os.makedirs(os.path.join(local_path, os.environ['DATA_PATH'], KB_UPDATE_FOLDER_NAME), exist_ok=True)
    file_path = os.path.join(local_path, os.environ['DATA_PATH'], KB_UPDATE_FOLDER_NAME, "KB Updated.txt")
    open(file_path, "w").close()
    with open(file_path, "a") as file:
        for i in tqdm(range(len(df_yes_to_update))):
            query = df_yes_to_update.loc[i, QUERY_ENG]
            response = df_yes_to_update.loc[i, RESPONSE]
            file.write(f"* {query.strip()}\n{response.strip()}\n\n")
    return True

def create_or_add_to_raw_kb_update_file(df, local_path):
    df.reset_index(drop=True, inplace=True)
    os.makedirs(os.path.join(local_path, os.environ['DATA_PATH'], RAW_DOCUMENTS_FOLDER_NAME), exist_ok=True)
    file_path = os.path.join(local_path, os.environ['DATA_PATH'], RAW_DOCUMENTS_FOLDER_NAME, "KB Updated.txt")
    if not os.path.exists(file_path):
        open(file_path, "w").close()
        data = utils.pull_sheet_data(SCOPES, SPREADSHEET_ID, STORE_RANGE_NAME, local_path)
        df = utils.convert_to_dataframe(data)
    
    df_yes_to_update = df[(df[ADD_TO_KB].str.strip().str.upper() == 'YES')]
    df_no_to_update = df[(df[ADD_TO_KB].str.strip().str.upper() == 'NO')]
    file = open(os.path.join(local_path, os.environ['DATA_PATH'], RAW_DOCUMENTS_FOLDER_NAME, "KB Updated.txt"), "a")
    df_yes_to_update.reset_index(drop=True, inplace=True)
    with open(file_path, "a") as file:
        for i in tqdm(range(len(df_yes_to_update))):
            query = df_yes_to_update.loc[i, QUERY_ENG]
            response = df_yes_to_update.loc[i, RESPONSE]
            logger.add_log(
                sender_id="KB updater",
                receiver_id="Bot",
                message_id=None,
                action_type="Updating KnowledgeBase",
                details={
                    "update_request_date": df_yes_to_update.loc[i, UPDATE_REQUEST_DATE],
                    "updated_date": df_yes_to_update.loc[i, UPDATED_DATE],
                    "query": query,
                    "updated_response": response,
                    "To Update KB": "YES",
                },
                timestamp=datetime.datetime.now(),
            )
            file.write(f"##\n{query.strip()}\n{response.strip()}\n\n")
    
    df_no_to_update.reset_index(drop=True, inplace=True)
    for i in tqdm(range(len(df_no_to_update))):
        query = df_no_to_update.loc[i, QUERY_ENG]
        response = df_no_to_update.loc[i, RESPONSE]
        logger.add_log(
            sender_id="KB updater",
            receiver_id="Bot",
            message_id=None,
            action_type="Not Updating KnowledgeBase",
            details={
                "update_request_date": df_no_to_update.loc[i, UPDATE_REQUEST_DATE],
                "updated_date": df_no_to_update.loc[i, UPDATED_DATE],
                "query": query,
                "response": response,
                "To Update KB": "NO",
            },
            timestamp=datetime.datetime.now(),
        )

def update_kb(is_created, updated_date, last_update_request_date):
    msg = ""
    if is_created:
        knowledge_base = KnowledgeBase(config)
        try:
            knowledge_base.update_kb_wa()
            msg = f"KB updated successfully on {updated_date} for update requests on {last_update_request_date}"
            print("KB updated successfully")
        except Exception as e:
            msg = f"Error updating KB: {e}"
    msg = f"No new updates to KB on {updated_date} for update requests on {last_update_request_date}"
    return msg

last_update_range_name = LAST_UPDATE_RANGE_NAME
df_yes_to_update, df_no_to_update, last_update_range_name = get_answered_questions_from_last_update(last_update_range_name, local_path)
if last_update_range_name is None:
    print("No update sheet found")
    sys.exit()
print(f"Last update sheet found: {last_update_range_name}")
last_update_request_date = utils.extract_date(last_update_range_name).strftime("%d-%m-%Y")
updated_date = datetime.datetime.now().strftime("%d-%m-%Y")
df_answered = pd.concat([df_yes_to_update, df_no_to_update])
df_answered[UPDATE_REQUEST_DATE] = last_update_request_date
df_answered[UPDATED_DATE] = updated_date
df_answered = df_answered[[UPDATE_REQUEST_DATE, UPDATED_DATE, QUERY_SOURCE_LANG, QUERY_ENG, RESPONSE, ADD_TO_KB, RELEVANT_DOC]]
df_answered.reset_index(drop=True, inplace=True)

is_created = create_kb_update_file(df_yes_to_update, local_path)
msg = update_kb(is_created, updated_date, last_update_request_date)

if not utils.is_sheet_present(SCOPES, SPREADSHEET_ID, STORE_RANGE_NAME, local_path):
    utils.create_sheet(SCOPES, SPREADSHEET_ID, STORE_RANGE_NAME, local_path)
    utils.add_headers(SCOPES, SPREADSHEET_ID, STORE_RANGE_NAME, [UPDATE_REQUEST_DATE, UPDATED_DATE, QUERY_SOURCE_LANG, QUERY_ENG, RESPONSE, ADD_TO_KB, RELEVANT_DOC], local_path)
utils.append_rows(SCOPES, SPREADSHEET_ID, STORE_RANGE_NAME, df_answered, local_path)

create_or_add_to_raw_kb_update_file(df_answered, local_path)

# send_email(msg)