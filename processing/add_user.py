import pymongo
import certifi
import os
import yaml

config_path = os.path.join(os.environ['APP_PATH'], "config.yaml")
with open(config_path, 'r') as data:
    config = yaml.safe_load(data)

import sys
sys.path.append(os.path.join(os.environ['APP_PATH'], 'src'))


from database.user_db import UserDB
from database.user_relation_db import UserRelationDB

from onboard import onboard_wa_helper, onboard_template
from messenger import WhatsappMessenger
from conversation_database import LoggingDatabase

user_db = UserDB(config)
user_relation_db = UserRelationDB(config)
logger = LoggingDatabase(config)


messenger = WhatsappMessenger(config, logger)

# onboard_template(config, logger, user, messenger)



from uuid import uuid4

user_id = str(uuid4())

phone_numbers2role = {
    "919876543210": "asha",
}

for i, phone_num in enumerate(phone_numbers2role):
    role = phone_numbers2role[phone_num]
    user = {
        'user_id': str(uuid4()),
        'user_language': 'hi',
        'whatsapp_id': phone_num,
        'user_type': role
    }
    print(user)
    user_db.insert_row(user['user_id'], user['whatsapp_id'], user['user_type'], user['user_language'])
    onboard_template(config, logger, user, messenger)