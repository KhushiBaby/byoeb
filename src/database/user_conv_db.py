import os
import datetime
import pymongo
import certifi

from database.base import BaseDB

class UserConvDB(BaseDB):
    def __init__(self, config):
        super().__init__(config)
        self.collection = self.db[config['COSMOS_USER_CONV_COLLECTION']]

    def insert_row(self,
        user_id,
        message_id,
        message_type,
        message_source_lang,
        source_language,
        message_translated,
        audio_blob_path,
        message_timestamp,
        reply_id=None):

        user_conv = {
            'user_id': user_id,
            'message_id': message_id,
            'message_type': message_type,
            'message_source_lang': message_source_lang,
            'source_language': source_language,
            'message_english': message_translated,
            'audio_blob_path': audio_blob_path,
            'message_timestamp': message_timestamp,
            'reply_id': reply_id,
        }
        db_id = self.collection.insert_one(user_conv)
        return db_id
    
    def get_from_db_id(self, db_id):
        user_conv = self.collection.find_one({'_id': db_id})
        return user_conv

    def get_from_message_id(self, message_id):
        user_conv = self.collection.find_one({'message_id': message_id})
        return user_conv
    
    def get_all_user_conv(self, user_id, message_type=None):
        if message_type is None:
            user_conv = self.collection.find({'user_id': user_id})
        else:
            user_conv = self.collection.find({'$and': [{'user_id': user_id}, {'message_type': message_type}]})
        user_conv = list(user_conv)
        return user_conv

    def add_llm_response(self,
        message_id,
        query_type,
        llm_response,
        citations):

        self.collection.update_one(
            {'message_id': message_id},
            {'$set': {
                'llm_response': llm_response,
                'citations': citations,
                'query_type': query_type
            }}
        )

    def add_query_type(self, message_id, query_type):
        self.collection.update_one(
            {'message_id': message_id},
            {'$set': {
                'query_type': query_type
            }}
        )

    def mark_resolved(self, message_id):
        self.collection.update_one(
            {'message_id': message_id},
            {'$set': {
                'resolved': True
            }}
        )

    def get_all_unresolved(self, from_ts, to_ts):
        user_conv = self.collection.find({"$and": [{"resolved": {"$ne": True}}, {'message_timestamp': {'$gte': from_ts, '$lt': to_ts}}]})
        return user_conv
    
    def get_most_recent_query(self, user_id):
        query_types = ['text', 'audio', 'interactive']
        user_conv = self.collection.find({'$and': [{'user_id': user_id}, {'message_type': {'$in': query_types}}]})
        user_conv = list(user_conv)
        user_conv = sorted(user_conv, key=lambda x: x['message_timestamp'], reverse=True)
        return user_conv[0] if len(user_conv) > 0 else None
    
    def get_most_recent_message(self, user_id):
        user_conv = self.collection.find({'user_id': user_id})
        user_conv = list(user_conv)
        user_conv = sorted(user_conv, key=lambda x: x['message_timestamp'], reverse=True)
        return user_conv[0] if len(user_conv) > 0 else None
    
    def get_from_reply_id(self, reply_id):
        user_conv = self.collection.find_one({'reply_id': reply_id})
        return user_conv