import os
import datetime
import pymongo
import certifi
import random
from cachetools import cached, TTLCache
from database.base import BaseDB

eight_hours = 8 * 60 * 60
cache = TTLCache(maxsize=100, ttl=eight_hours)

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
    
    @cached(cache)
    def _get_expert_rows(self, expert_type, test=False):
        print("Executing query")
        if test:
            match_query = {'$and': [{'user_type': expert_type}, {'test_user': True}, {'opt out': {'$ne': True}}]}
        else:
            match_query = {'$and': [{'user_type': expert_type}, {'test_user': {'$ne': True}}, {'opt out': {'$ne': True}}]}
        
        rows = list(self.collection.find(match_query))
        return rows
    
    def get_random_expert(self, expert_type, numbers_of_experts, test=False):
        rows = self._get_expert_rows(expert_type, test)
        print(len(rows))
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