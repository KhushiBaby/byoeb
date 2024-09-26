import os
import datetime
import pymongo
import certifi

from database.base import BaseDB

class ExpertConvDB(BaseDB):
    def __init__(self, config):
        super().__init__(config)
        self.collection = self.db[config['COSMOS_EXPERT_CONV_COLLECTION']]

    def insert_row(self,
        user_id,
        message_id,
        message_type,
        message_modality,
        message,
        reply_id,
        message_timestamp,
        transaction_message_id):

        row = {
            'user_id': user_id,
            'message_id': message_id,
            'message_type': message_type,
            'message_modality': message_modality,
            'message': message,
            'reply_id': reply_id,
            'message_timestamp': message_timestamp,
            'transaction_message_id': transaction_message_id
        }

        db_id = self.collection.insert_one(row)
        return db_id
    
    def get_from_message_id(self, message_id):
        row = self.collection.find_one({'message_id': message_id})
        return row
    
    def get_from_transaction_message_id(self, transaction_message_id, message_type=None):
        if message_type:
            rows = self.collection.find({'$and': [{'transaction_message_id': transaction_message_id}, {'message_type': message_type}]})
        else:
            rows = self.collection.find({'transaction_message_id': transaction_message_id})
        rows = list(rows)
        return rows
    
    def find_all_with_transaction_id_and_receiver_id(self, transaction_message_id, user_id, message_type=None):
        if message_type:
            rows = self.collection.find({'$and': [{'transaction_message_id': transaction_message_id}, {'user_id': user_id}, {'message_type': message_type}]})
        else:
            rows = self.collection.find({'$and': [{'transaction_message_id': transaction_message_id}, {'user_id': user_id}]})
        rows = list(rows)
        return rows
    
    def get_all_messages_with_message_type(self, message_type):
        rows = self.collection.find({'message_type': message_type})
        rows = list(rows)
        return rows
    
    def find_all(self, message_type=None):
        if message_type:
            rows = self.collection.find({'message_type': message_type})
        else:
            rows = self.collection.find()
        rows = list(rows)
        return rows
    
    def find_all_with_duration(self, message_type, from_ts, to_ts):
        if message_type:
            rows = self.collection.find({'$and': [{'message_type': message_type}, {'message_timestamp': {'$gte': from_ts}}, {'message_timestamp': {'$lt': to_ts}}]})
        else:
            rows = self.collection.find({'$and': [{'message_timestamp': {'$gte': from_ts}}, {'message_timestamp': {'$lt': to_ts}}]})
        rows = list(rows)
        return rows
    
    
