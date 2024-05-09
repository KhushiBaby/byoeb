import psutil
import yaml

import os
print('Code started running')
local_path = os.environ['APP_PATH']
import sys
sys.path.append(local_path + '/src')
from knowledge_base import KnowledgeBase
from conversation_database import LoggingDatabase
from database import UserDB, UserConvDB, BotConvDB, ExpertConvDB, UserRelationDB
import pandas as pd
import ast
from typing import Any
from tqdm import tqdm
from datetime import datetime

with open(os.path.join(local_path,'config.yaml')) as file:    
    config = yaml.load(file, Loader=yaml.FullLoader)

user_db = UserDB(config)
user_conv_db = UserConvDB(config)
bot_conv_db = BotConvDB(config)



os.makedirs(os.path.join(local_path, os.environ['DATA_PATH'], "kb_update_raw"), exist_ok=True)
open(os.path.join(local_path, os.environ['DATA_PATH'], "kb_update_raw/KB Updated.txt"), "w").close()
myfile = open(os.path.join(local_path, os.environ['DATA_PATH'], "kb_update_raw/KB Updated.txt"), "w")
rawfile = open(os.path.join(local_path, os.environ['DATA_PATH'], "raw_documents/KB Updated.txt"), "w")


test_users = user_db.collection.find({'test_user': True})
test_users_df = pd.DataFrame(list(test_users))

user_conv_cursor = user_conv_db.collection.find()
bot_conv_cursor = bot_conv_db.collection.find({'message_type': 'query_consensus_response'})

user_conv_df = pd.DataFrame(list(user_conv_cursor))
bot_conv_df = pd.DataFrame(list(bot_conv_cursor))

# join user_conv_df and bot_conv_df on 'transaction_message_id'

user_conv_df['transaction_message_id'] = user_conv_df['message_id'].astype(str)
bot_conv_df['transaction_message_id'] = bot_conv_df['transaction_message_id'].astype(str)

kb_updates = pd.merge(user_conv_df, bot_conv_df, on='transaction_message_id', how='inner')

# filter out active users
kb_updates = kb_updates[~kb_updates['user_id'].isin(test_users_df['user_id'])]


for i, row in tqdm(kb_updates.iterrows()):
    print(row['message_english_x'], row['message_english_y'])
    myfile.write("##\n")
    rawfile.write("##\n")
    myfile.write(row['message_english_x'] + '\n')
    rawfile.write(row['message_english_x'] + '\n')
    myfile.write(row['message_english_y'] + '\n')
    rawfile.write(row['message_english_y'] + '\n')

myfile.close()
rawfile.close()

knowledge_base = KnowledgeBase(config)
print(repr(knowledge_base.config['PROJECT_NAME']))
knowledge_base.create_embeddings()
print('KB updated successfully')

