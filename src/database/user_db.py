import os
import datetime
import pymongo
import certifi
import random

from database.base import BaseDB

class UserDB(BaseDB):
    def __init__(self, config):
        super().__init__(config)
        self.collection = self.db[config['COSMOS_USER_COLLECTION']]

    def insert_row(self,
        user_id,
        whatsapp_id,
        user_type,
        user_language,
        test_user=False):

        user = {
            'user_id': user_id,
            'whatsapp_id': whatsapp_id,
            'user_type': user_type,
            'user_language': user_language,
            'timestamp' : datetime.datetime.now(),
            'test_user': test_user
        }
        db_id = self.collection.insert_one(user)
        return db_id
    
    def get_from_user_id(self, user_id):
        user = self.collection.find_one({'user_id': user_id})
        return user
    
    def get_from_whatsapp_id(self, whatsapp_id):
        user = self.collection.find_one({'whatsapp_id': whatsapp_id})
        return user
    
    def update_user_language(self, user_id, user_language):
        self.collection.update_one(
            {'user_id': user_id},
            {'$set': {
                'user_language': user_language
            }}
        )

    def mark_user_opted_out(self, user_id):
        self.collection.update_one(
            {'user_id': user_id},
            {'$set': {
                'opt out': True
            }}
        )
        return
    
    def mark_user_opted_in(self, user_id):
        self.collection.update_one(
            {'user_id': user_id},
            {'$set': {
                'opt out': False
            }}
        )
        return
    
    def get_random_expert(self, expert_type, numbers_of_experts, bot_conv_db, test=False):
        if test:
            rows = list(self.collection.find({'$and': [{'user_type':expert_type}, {'test_user':True}, {'opt out' :{'$ne':True}}]}))
        else:   
            rows = list(self.collection.find({'$and': [{'user_type':expert_type}, {'test_user':{'$ne':True}}, {'opt out' :{'$ne':True}}]}))
        if len(rows) < numbers_of_experts:
            return rows
        #for every expert, find the number of messages sent to them in the last 24 hours
        from_ts = datetime.datetime.now() - datetime.timedelta(hours=24)
        to_ts = datetime.datetime.now()
        for row in rows:
            user_id = row['user_id']
            expert_conv = bot_conv_db.find_with_receiver_id_and_duration(user_id, "response_request", from_ts, to_ts)
            #find unique number of transaction message ids
            expert_conv = list(expert_conv)
            expert_conv = [conv['transaction_message_id'] for conv in expert_conv]
            expert_conv = set(expert_conv)
            row['number_of_messages'] = len(expert_conv)

        #sort the experts based on the number of messages
        rows = sorted(rows, key = lambda i: i['number_of_messages'])

        #filter experts with less than 3 messages
        filtered_rows = [row for row in rows if row['number_of_messages'] < 3]

        if len(filtered_rows) >= numbers_of_experts:
            random_experts = random.sample(filtered_rows, numbers_of_experts)
        
        else:
            random_experts = rows[:numbers_of_experts]

        
        return random_experts
        
    
    def get_all_users(self, user_type=None):
        if user_type is None:
            users = self.collection.find({})
        else:
            users = self.collection.find({'user_type': user_type})
        users = list(users)
        return users