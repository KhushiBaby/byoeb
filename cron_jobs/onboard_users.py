from io import BytesIO
import os
from datetime import datetime
from azure.storage.blob import BlobServiceClient
import yaml
import sys
local_path = os.environ["APP_PATH"]
sys.path.append(local_path.strip() + "/src")

with open(local_path + "/config.yaml") as file:
    config = yaml.load(file, Loader=yaml.FullLoader)

from messenger import WhatsappMessenger
from conversation_database import (
    LoggingDatabase,
)
from database import UserDB, BotConvDB, AppLogger
from uuid import uuid4

app_logger = AppLogger()
user_db = UserDB(config)
bot_conv_db = BotConvDB(config)
messenger = WhatsappMessenger(config, app_logger)

from onboard import onboard_template


container_name = os.environ["USER_FILE_CONTAINER"]
excel_path = os.environ["USER_FILE_PATH"]

#create blob client

blob_service_client = BlobServiceClient.from_connection_string(os.environ["AZURE_STORAGE_CONNECTION_STRING"])
blob_client = blob_service_client.get_blob_client(container=container_name, blob=excel_path)

#download blob
downloaded_blob = blob_client.download_blob()

#load blob to pandas dataframe
import pandas as pd
# df = pd.read_excel(BytesIO(downloaded_blob.readall()))
# print(len(df))

df = pd.read_excel('/mnt/c/Users/b-bsachdeva/Downloads/List of new ASHAs & ANMs.xlsx')

cursor = user_db.collection.find({})
user_df = pd.DataFrame(list(cursor))

print(user_df['user_type'].value_counts())

df['whatsapp_id'] = df['whatsapp_id'].astype(str)
df['whatsapp_id_len'] = df['whatsapp_id'].apply(lambda x: len(x))

#if len == 10, add 91 to the beginning
df.loc[df['whatsapp_id_len'] == 10, 'whatsapp_id'] = '91' + df['whatsapp_id']
df['whatsapp_id_len'] = df['whatsapp_id'].apply(lambda x: len(x))

roles = {
    'asha':'Asha',
    'anm': 'ANM'
}

print(df['user_type'].value_counts())


# test_asha = user_db.get_from_whatsapp_id('918375066113')
# onboard_template(config, app_logger, test_asha, messenger, bot_conv_db)

new_users = {
    'Asha': 0,
    'ANM': 0
}

#print unique whatsapp ids
print(len(df['whatsapp_id'].unique()))

for i, row in df.iterrows():
    row['whatsapp_id'] = str(row['whatsapp_id']).strip()
    if row['whatsapp_id'] == '':
        continue
    if row['whatsapp_id'] in user_df['whatsapp_id'].values:
        # print(f"User with whatsapp_id {row['whatsapp_id']} already exists")
        continue
    
    
    user_id = str(uuid4())
    role = roles[row['user_type'].lower()]
    whatsapp_id = row['whatsapp_id'].strip()
    lang = row['user_language'].lower()[:2]
    new_users[role] += 1
    user = {
        'user_id': user_id,
        'whatsapp_id': whatsapp_id,
        'user_type': role,
        'user_language': lang
    }
    print("Adding new row", whatsapp_id)
    # user_db.insert_row(user['user_id'], user['whatsapp_id'], user['user_type'], user['user_language'])
    # onboard_template(config, app_logger, user, messenger, bot_conv_db)

    
print(new_users)
    
