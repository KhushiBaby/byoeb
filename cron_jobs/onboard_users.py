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
from database import UserDB
from uuid import uuid4

logger = LoggingDatabase(config)
user_db = UserDB(config)
messenger = WhatsappMessenger(config, logger)

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
df = pd.read_excel(BytesIO(downloaded_blob.readall()))

cursor = user_db.collection.find({})
user_df = pd.DataFrame(list(cursor))

print(user_df)

roles = {
    'asha':'Asha',
    'anm': 'ANM'
}


for i, row in df.iterrows():
    row['whatsapp_id'] = str(row['whatsapp_id']).strip()
    if row['whatsapp_id'] in user_df['whatsapp_id'].values:
        print(f"User with whatsapp_id {row['whatsapp_id']} already exists")
        continue

    user_id = str(uuid4())
    role = roles[row['user_type'].lower()]
    whatsapp_id = row['whatsapp_id'].strip()
    lang = row['user_language'].lower()[:2]

    user = {
        'user_id': user_id,
        'whatsapp_id': whatsapp_id,
        'user_type': role,
        'user_language': lang
    }

    user_db.insert_row(user['user_id'], user['whatsapp_id'], user['user_type'], user['user_language'])
    onboard_template(config, logger, user, messenger)


    
    
