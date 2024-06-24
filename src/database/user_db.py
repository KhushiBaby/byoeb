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
    
    def dep_get_random_expert(self, expert_type, number_of_experts):
        pipeline = [
            {"$match": {"user_type": expert_type}},
            {"$sample": {"size": number_of_experts}}
        ]
        experts = list(self.collection.aggregate(pipeline))
        return experts
    
    def get_random_expert(self, expert_type, numbers_of_experts, test=False):
        if test:
            rows = list(self.collection.find({'$and': [{'user_type':expert_type}, {'test_user':True}]}))
        else:   
            rows = list(self.collection.find({'$and': [{'user_type':expert_type}, {'test_user':{'$ne':True}}]}))
        if len(rows) < numbers_of_experts:
            return rows
        random_experts = random.sample(rows, numbers_of_experts)
        return random_experts
    
    def get_all_users(self, user_type=None):
        if user_type is None:
            users = self.collection.find({})
        else:
            users = self.collection.find({'user_type': user_type})
        users = list(users)
        return users