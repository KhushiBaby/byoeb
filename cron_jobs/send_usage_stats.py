
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
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

def extract_onboarding_count(onboarding_responses_df, user_ids):
    num_yes = len(user_ids.intersection(set(onboarding_responses_df[onboarding_responses_df["message_source_lang"] == "हाँ"]["user_id"])))
    num_no = len(user_ids.intersection(set(onboarding_responses_df[onboarding_responses_df["message_source_lang"] == "नहीं"]["user_id"])))
    return num_yes, num_no

def get_message_wise_stats(messages_df, user_ids):
    message_count = messages_df[messages_df["user_id"].isin(user_ids)].groupby("user_id").size().to_dict()
    first_message = messages_df[messages_df["user_id"].isin(user_ids)].groupby("user_id")["message_timestamp"].min().to_dict()
    return message_count, first_message

NUM_DAYS = 3
NUM_HOURS = NUM_DAYS*24


user_db = UserDB(config)
user_conv_db = UserConvDB(config)
bot_conv_db = BotConvDB(config)
expert_conv_db = ExpertConvDB(config)
app_logger = AppLogger()
messenger = WhatsappMessenger(config, app_logger)

asha_list = user_db.get_all_users(user_type="Asha")
anm_list = user_db.get_all_users(user_type="ANM")

asha_list = [asha for asha in asha_list if asha.get("test_user", False) == False]
anm_list = [anm for anm in anm_list if anm.get("test_user", False) == False]

# print(len(asha_list), len(anm_list))
asha_user_ids = set([asha["user_id"] for asha in asha_list])
anm_user_ids = set([anm["user_id"] for anm in anm_list])

dt_now = datetime.datetime.now()
dt_from = dt_now - datetime.timedelta(hours=NUM_HOURS)

onboard_responses = user_conv_db.get_all_queries_with_message_type("onboarding_response")

onboard_responses_df = pd.DataFrame(onboard_responses)
onboard_responses_df = onboard_responses_df.sort_values('message_timestamp').drop_duplicates('user_id', keep='last')

onboard_responses_delta_days_df = onboard_responses_df[onboard_responses_df["message_timestamp"] > dt_from]

num_asha_yes, num_asha_no = extract_onboarding_count(onboard_responses_df, asha_user_ids)
num_asha_yes_delta_days, num_asha_no_delta_days = extract_onboarding_count(onboard_responses_delta_days_df, asha_user_ids)
num_anm_yes, _ = extract_onboarding_count(onboard_responses_df, anm_user_ids)
num_anm_yes_delta_days, _ = extract_onboarding_count(onboard_responses_delta_days_df, anm_user_ids)

overall_stat_table = []


overall_stat_table.append(["Total ASHAs Consented 'Yes'", f"{num_asha_yes} [Increase in last {NUM_HOURS} hours: {num_asha_yes_delta_days}]"])
overall_stat_table.append(["Total ASHAs Consented 'No'", f"{num_asha_no} [Increase in last {NUM_HOURS} hours: {num_asha_no_delta_days}]"])
overall_stat_table.append(["Total ANMs Consented 'Yes'", f"{num_anm_yes} [Increase in last {NUM_HOURS} hours: {num_anm_yes_delta_days}]"])

messages = user_conv_db.get_all_queries()
messages_df = pd.DataFrame(messages)
messages_delta_df = messages_df[messages_df["message_timestamp"] > dt_from]

asha_message_count, asha_first_msg = get_message_wise_stats(messages_df, asha_user_ids)
asha_message_count_delta_days, _ = get_message_wise_stats(messages_delta_df, asha_user_ids)
asha_first_msg_delta_days = {k: v for k, v in asha_first_msg.items() if v > dt_from}


overall_stat_table.append(["ASHAs who sent 1+ messages", f"{len(asha_first_msg)} [Increase in last {NUM_HOURS} hours: {len(asha_first_msg_delta_days)}]"])

anm_messages = expert_conv_db.get_all_messages_with_message_type("consensus_response")
anm_messages_df = pd.DataFrame(anm_messages)
anm_messages_delta_df = anm_messages_df[anm_messages_df["message_timestamp"] > dt_from]

anm_message_count, anm_first_msg = get_message_wise_stats(anm_messages_df, anm_user_ids)
anm_message_count_delta_days, _ = get_message_wise_stats(anm_messages_delta_df, anm_user_ids)
anm_first_msg_delta_days = {k: v for k, v in anm_first_msg.items() if v > dt_from}


overall_stat_table.append(["ANMs who sent 1+ messages", f"{len(anm_first_msg)} [Increase in last {NUM_HOURS} hours: {len(anm_first_msg_delta_days)}]"])
overall_stat_table.append([f"ASHAs active in the last {NUM_HOURS} hours", f"{len(asha_message_count_delta_days)}"])
overall_stat_table.append([f"ANMs active in the last {NUM_HOURS} hours", f"{len(anm_message_count_delta_days)}"])

overall_stat_table.append([f"Total messages sent by ASHAs", f"{len(messages_df)} [Increase in last {NUM_HOURS} hours: {len(messages_delta_df)}]"])
overall_stat_table.append([f"Total messages sent by ANMs", f"{len(anm_messages_df)} [Increase in last {NUM_HOURS} hours: {len(anm_messages_delta_df)}]"])


#sort ashas by message count
asha_message_count_delta_days = dict(sorted(asha_message_count_delta_days.items(), key=lambda item: item[1], reverse=True))
asha_table_data = [
    ["User ID", f"Messages in last {NUM_HOURS} hours", "Total messages"]
]

for i in range(min(10, len(asha_message_count_delta_days))):
    user_id = list(asha_message_count_delta_days.keys())[i]
    asha_table_data.append([user_id, asha_message_count_delta_days[user_id], asha_message_count[user_id]])

anm_message_count_delta_days = dict(sorted(anm_message_count_delta_days.items(), key=lambda item: item[1], reverse=True))

anm_table_data = [
    ["User ID", f"Messages in last {NUM_HOURS} hours", "Total messages"]
]

for i in range(min(10, len(anm_message_count_delta_days))):
    user_id = list(anm_message_count_delta_days.keys())[i]
    anm_table_data.append([user_id, anm_message_count_delta_days[user_id], anm_message_count[user_id]])


date_today = datetime.datetime.now().strftime("%d-%m-%Y")

credential = DefaultAzureCredential()
client = SecretClient(vault_url=os.environ['AZ_KEY_VAULT_URL'].strip(), credential=credential)
secret = client.get_secret("logging-email-list")
email_list = eval(secret.value)

s = smtplib.SMTP('smtp.gmail.com', 587)
s.starttls()
s.login(os.environ['LOGGING_EMAIL_ID'].strip(), os.environ['LOGGING_EMAIL_PASS'].strip())
spreadsheet_id = os.environ['SPREADSHEET_ID'].strip()

html_message = f"""
<html>
<head>
    <style>
    table {{
        border-collapse: collapse;
        width: 100%;
    }}
    th, td {{
        border: 1px solid black;
        padding: 10px;  /* Increase padding to increase space between columns */
        text-align: left;
    }}
</style>
</head>
    <body style="font-family: 'Courier New', Courier, monospace;">
    Hello,<br>
    Here is the link to the usage logs: <a href="https://docs.google.com/spreadsheets/d/{spreadsheet_id}/view?gid=1306836655#gid=1306836655">Usage Logs</a><br>
    Please find ASHABot Usage stats below.<br>
    <h4>Overall Stats:</h4>
    {tabulate(overall_stat_table, tablefmt="html", colalign=("left", "left"))}
    <h4>Top 10 active ASHAs in last {NUM_HOURS} hours:</h4>
    {tabulate(asha_table_data, headers="firstrow", tablefmt="html", colalign=("left", "left", "left"))}
    <h4>Top 10 active ANMs in last {NUM_HOURS} hours:</h4>
    {tabulate(anm_table_data, headers="firstrow", tablefmt="html", colalign=("left", "left", "left"))}
    <br>
    Regards,<br>
    ASHABot Team
</body>
</html>
"""

# Create the email
msg = MIMEMultipart('alternative')
msg['Subject'] = f"ASHABot usage stats for {date_today}"
msg['From'] = "sankarabotmsr@gmail.com"
msg['To'] = ", ".join(email_list)

# Attach the HTML message
msg.attach(MIMEText(html_message, 'html'))


#save the email to an html file
with open('email.html', 'w') as file:
    file.write(msg.as_string())

print("Sending email to: ", email_list)

for receiver in email_list:
    s.sendmail("sankarabotmsr@gmail.com", receiver, msg.as_string())
s.quit()