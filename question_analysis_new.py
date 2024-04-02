import os
import sys
import yaml
import pandas as pd

sys.path.append('src/')

from azure_language_tools import translator

import json
from knowledge_base import KnowledgeBase
from conversation_database import (
    ConversationDatabase,
    LongTermDatabase,
    LoggingDatabase,
)

with open('config.yaml', 'r') as f:
    config = yaml.safe_load(f)

logger = LoggingDatabase(config)

translator = translator()
knowledge_base = KnowledgeBase(config)


excel_path = '/mnt/c/Users/b-bsachdeva/Downloads/ASHA training gaps 2024.xlsx'
excel_save_path = '/mnt/c/Users/b-bsachdeva/Downloads/ASHA training gaps 2024_processed.xlsx'
file = pd.ExcelFile(excel_path)


processed_dataframe = pd.DataFrame(columns=['Question', 'Translated question', 'GPT Output', 'Retrieved Documents', 'Sources'])
sheet_name = 'Final Qs'
df = file.parse(sheet_name)

for i, row in df.iterrows():
    #pick from row['Finalized Questions (English / Hinglish)'] or row['Finalized Questions (Hindi)']
    question = row['Finalized Questions (English / Hinglish)'] if pd.notna(row['Finalized Questions (English / Hinglish)']) else row['Finalized Questions (Hindi)']
    print(question)
    #use azure transalte api to translate the question to english
    eng_question = translator.translate_text(question, 'hi', 'en', logger)
    print(eng_question)
    
    #use knowledge base to get the answer
    
    
    gpt_output, citations, query_type, relevant_chunks = knowledge_base.answer_query_text(question, eng_question, logger)
    print(gpt_output)
    # processed_dataframe.iloc[len(processed_dataframe)] = [question, eng_question, gpt_output, relevant_chunks, citations]
    
    documents = relevant_chunks['documents']
    sources = relevant_chunks['metadatas']
    processed_dataframe = processed_dataframe._append({'Question': question, 'Translated question': eng_question, 'GPT Output': gpt_output, 'Retrieved Documents': documents, 'Sources': sources}, ignore_index=True)

print(processed_dataframe)


# Create a new sheet
writer = pd.ExcelWriter(excel_save_path, engine='openpyxl', mode='a', if_sheet_exists='replace')

# Assuming that df is the DataFrame you want to write to the new sheet
processed_dataframe.to_excel(writer, sheet_name='KB all updated', index=False)

# Save the changes
writer.close()

