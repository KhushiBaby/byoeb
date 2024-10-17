
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
import utils

ASHA_LOGS_RANGE_NAME = 'ASHA logs'
ANM_LOGS_RANGE_NAME = 'ANM logs'
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
SPREADSHEET_ID = os.environ['SPREADSHEET_ID'].strip()

user_db = UserDB(config)
user_conv_db = UserConvDB(config)
bot_conv_db = BotConvDB(config)
expert_conv_db = ExpertConvDB(config)
logger = AppLogger()

users = user_db.get_all_users()
users_df = pd.DataFrame(users)
users_df = users_df[(users_df['test_user'] != True)]

dt_now = datetime.datetime.now()
cut_off_dt = pd.to_datetime('2024-08-01 00:00:00')

all_queries = user_conv_db.get_all_queries_in_duration(cut_off_dt, dt_now)
user_conv_df = pd.DataFrame(all_queries)
user_conv_df = user_conv_df[user_conv_df['message_type'] != 'onboarding_response']
user_conv_df.sort_values('message_timestamp', inplace=True, ascending=False)
user_conv_df = user_conv_df[user_conv_df['user_id'].isin(users_df['user_id'])]

cursor = bot_conv_db.find_all(message_type='query_response')
query_responses_df = pd.DataFrame(list(cursor))

user_conv_rename = {
    'message_id': 'transaction_message_id',
    'message_source_lang': 'query_source_lang',
    'message_english' : 'query_english',
    'source_language': 'query_language',
    'audio_blob_path' : 'query_audio_blob_path',
    'message_timestamp': 'query_timestamp',
    'message_type': 'query_message_type'
}
user_conv_df.rename(columns=user_conv_rename, inplace=True)
bot_conv_rename = {
    'message_source_lang': 'response_source_lang',
    'message_language': 'response_language',
    'message_english' : 'response_english',
    'message_timestamp': 'response_timestamp',
}
query_responses_df.rename(columns=bot_conv_rename, inplace=True)

feedback_responses = user_conv_df[user_conv_df['query_message_type'] == 'feedback_response']
user_conv_df = user_conv_df[user_conv_df['query_message_type'] != 'feedback_response']

#drop if query_source_lang == 'onboard-asha'

user_conv_df = user_conv_df[user_conv_df['query_source_lang'] != 'onboard-asha']

print(len(user_conv_df))
user_conv_df = user_conv_df.merge(query_responses_df, on='transaction_message_id', how='left')
print(len(user_conv_df))

empty_audio_responses = bot_conv_db.find_all(message_type='empty_audio_response')
empty_audio_responses_df = pd.DataFrame(list(empty_audio_responses))
print(empty_audio_responses_df.head())

user_conv_df['response_english'] = user_conv_df.apply(lambda x: 'empty_audio_response' if x['transaction_message_id'] in empty_audio_responses_df['transaction_message_id'].values else x['response_english'], axis=1)


cursor = expert_conv_db.find_all(message_type='consensus_response')
expert_consensus_responses_df = pd.DataFrame(list(cursor))

#remove is user_id not in users_df
expert_consensus_responses_df = expert_consensus_responses_df[expert_consensus_responses_df['user_id'].isin(users_df['user_id'])]
expert_consensus_responses_df

# create a column in user_conv_df to store the expert_consensus_responses as a list of tuples (expert_id, response, timestamp)
expert_consensus_responses_df['expert_consensus_response'] = expert_consensus_responses_df.apply(lambda x: (x['user_id'], x['message'], x['message_timestamp']), axis=1)
expert_consensus_responses_df = expert_consensus_responses_df.groupby('transaction_message_id')['expert_consensus_response'].apply(list).reset_index()
#display entire dataframe, max width
pd.set_option('display.max_colwidth', None)
print(len(expert_consensus_responses_df))

#join the expert_consensus_responses_df with user_conv_df on transaction_message_id
if len(expert_consensus_responses_df) > 0:
    user_conv_df = user_conv_df.merge(expert_consensus_responses_df, on='transaction_message_id', how='left')

    # cursor = expert_conv_collection.find({'message_type': 'consensus_response'})
    cursor = expert_conv_db.find_all(message_type='consensus_response')
    expert_consensus_responses_df = pd.DataFrame(list(cursor))

    #remove is user_id not in users_df
    expert_consensus_responses_df = expert_consensus_responses_df[expert_consensus_responses_df['user_id'].isin(users_df['user_id'])]


# cursor = bot_conv_db.find({"message_type": 'query_consensus_response'})
cursor = bot_conv_db.find_all(message_type='query_consensus_response')
consensus_responses_df = pd.DataFrame(list(cursor))
consensus_responses_df = consensus_responses_df[consensus_responses_df['receiver_id'].isin(users_df['user_id'])]
consensur_rename = {
    'message_source_lang': 'consensus_response_source_lang',
    'message_language': 'consensus_response_language',
    'message_english' : 'consensus_response_english',
    'message_timestamp': 'consensus_response_timestamp',
    'citations' : 'consensus_citations',
}

consensus_responses_df.rename(columns=consensur_rename, inplace=True)
print(consensus_responses_df.columns)
print(len(consensus_responses_df))
if len(consensus_responses_df) > 0:
    user_conv_df = user_conv_df.merge(consensus_responses_df, on='transaction_message_id', how='left')
    
    def extract_cited_messages(citations):
        if pd.isna(citations) or citations == '':
            return None
        citations = citations.strip()
        citations = citations.replace('expert_consensus: ', '')
        citations = citations.split(', ')
        msg_rows = expert_consensus_responses_df[expert_consensus_responses_df['message_id'].isin(citations)]
        cited_messages = msg_rows.apply(lambda x: (x['user_id'], x['message'], x['message_timestamp']), axis=1).tolist()
        return cited_messages

    user_conv_df['cited_messages'] = user_conv_df['consensus_citations'].apply(extract_cited_messages)


print(user_conv_df.columns)
columns_to_save = ['user_id', 'query_source_lang', 'query_english', 'query_message_type', 'query_type', 'query_timestamp', 'response_source_lang', 'response_english', 'citations', 'expert_consensus_response', 'consensus_response_source_lang',
    'consensus_response_english', 'cited_messages', 'consensus_response_timestamp']
# remove any columns not in user_conv_df.columns
columns_to_save = [col for col in columns_to_save if col in user_conv_df.columns]
final_asha_df = user_conv_df[columns_to_save]

for col in final_asha_df.columns:
    if 'timestamp' in col:
        print(type(final_asha_df[col].iloc[0]))
        #convert to string
        final_asha_df[col] = pd.to_datetime(final_asha_df[col], errors='coerce')
        final_asha_df[col] = final_asha_df[col].dt.tz_localize('Asia/Kolkata').dt.strftime('%Y-%m-%d %H:%M:%S')
                                       

final_rename = {
    'user_id' : 'ASHA User ID',
    'query_message_type': 'Query Input Type',
    'query_source_lang': 'Query in Source Language (Hindi/Hinglish)',
    'query_english': 'Query in English (to GPT)',
    'query_type': 'Query Type',
    'query_timestamp': 'Query Timestamp',
    'response_source_lang': 'ASHABot Response in Source Language (Hindi/Hinglish)',
    'response_english': 'ASHABot Response in English',
    'response_timestamp': 'ASHABot Response Timestamp',
    'citations': 'Citations',
    'expert_consensus_response': 'ANM Responses',
    'consensus_response_source_lang': 'ASHABot Final Consensus Response in Source Language (Hindi/Hinglish)',
    'consensus_response_english': 'ASHABot Final Consensus Response in English',
    'cited_messages': 'ASHABot Final Consensus Citations',
    'consensus_response_timestamp': 'ASHABot Final Consensus Response Timestamp'
}
final_asha_df = final_asha_df.rename(columns=final_rename)
# final_asha_df.head()
#make sure all columns are string
final_asha_df = final_asha_df.astype(str)

#fill NaN with ''
final_asha_df = final_asha_df.fillna('')

#save to csv
# final_asha_df.to_csv(os.path.join(local_path, 'asha_bot_logs.csv'), index=False)



anm_responses = expert_conv_db.find_all_with_duration('consensus_response', cut_off_dt, dt_now)
anm_responses_df = pd.DataFrame(anm_responses)

#take only the anm responses from users_df
anm_responses_df = anm_responses_df[anm_responses_df['user_id'].isin(users_df['user_id'])]
anm_responses_df = anm_responses_df[~anm_responses_df['user_id'].isna()]
anm_responses_df = anm_responses_df[['user_id', 'message', 'message_type', 'message_timestamp', 'transaction_message_id']]
anm_responses_df = anm_responses_df.rename(columns = {
    'user_id': 'anm_user_id',
    'message': 'anm_message',
    'message_type': 'anm_message_type',
    'message_timestamp': 'anm_message_timestamp',
})

#sort by message_timestamp
anm_responses_df.sort_values('anm_message_timestamp', inplace=True, ascending=False)

asha_query_df = user_conv_df[['user_id', 'transaction_message_id', 'query_source_lang', 'query_english', 'query_type', 'query_message_type', 'query_timestamp']]
final_anm_df = anm_responses_df.merge(asha_query_df, on='transaction_message_id', how='left')

final_anm_df = final_anm_df.rename(columns = {
    'user_id': 'ANM User ID',
    'query_source_lang': 'ASHA Query Source Language',
    'query_english': 'ASHA Query English',
    'query_type': 'ASHA Query Type',
    'query_message_type': 'ASHA Query Input Type',
    'query_timestamp': 'ASHA Query Timestamp',
    'anm_message': 'ANM Response',
    'anm_message_timestamp': 'ANM Response Timestamp'
})

final_anm_df = final_anm_df[['ANM User ID', 'ASHA Query Source Language', 'ANM Response', 'ANM Response Timestamp', 'ASHA Query English', 'ASHA Query Type', 'ASHA Query Input Type', 'ASHA Query Timestamp']]

#drop if query_source_lang is nan
final_anm_df = final_anm_df[~final_anm_df['ASHA Query Source Language'].isna()]

#make sure all columns are string
final_anm_df = final_anm_df.astype(str)

#fill NaN with ''
final_anm_df = final_anm_df.fillna('')


utils.delete_all_rows(SCOPES, SPREADSHEET_ID, ASHA_LOGS_RANGE_NAME, local_path)
utils.delete_all_rows(SCOPES, SPREADSHEET_ID, ANM_LOGS_RANGE_NAME, local_path)

utils.add_rows(SCOPES, SPREADSHEET_ID, ASHA_LOGS_RANGE_NAME, final_asha_df, local_path)
utils.add_rows(SCOPES, SPREADSHEET_ID, ANM_LOGS_RANGE_NAME, final_anm_df, local_path)