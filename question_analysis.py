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

for sheet in file.sheet_names:
    df = file.parse(sheet)
    print(sheet, "Cleaned questions" in df.columns)

processed_dataframe = pd.DataFrame(columns=['Question', 'Translated question', 'GPT Output', 'Retrieved Documents', 'Sources'])

for sheet_name in file.sheet_names:
    df = file.parse(sheet_name)
    if "Cleaned questions" not in df.columns:
        print(f"Sheet {sheet_name} does not contain 'Cleaned questions' column")
        continue
    cleaned_questions = df[df.columns[df.columns.str.contains('Cleaned questions', case=False)]]
    print(len(cleaned_questions))
    #expand cleaned_questions as one cell may contain multiple comma separated questions
    cleaned_questions = cleaned_questions.stack().str.split(',').explode().reset_index(drop=True)
    cleaned_questions = cleaned_questions.str.strip()
    #remove empty strings
    cleaned_questions = cleaned_questions[cleaned_questions != '']

    for question in cleaned_questions:
        print(question)
        #use azure transalte api to translate the question to english
        eng_question = translator.translate_text(question, 'hi', 'en', logger)
        print(eng_question)
        
        #use knowledge base to get the answer
        
        
        gpt_output, citations, query_type, relevant_chunks = knowledge_base.answer_query_text(eng_question, logger)
        print(gpt_output)
        # processed_dataframe.iloc[len(processed_dataframe)] = [question, eng_question, gpt_output, relevant_chunks, citations]
        
        documents = relevant_chunks['documents']
        sources = relevant_chunks['metadatas']
        processed_dataframe = processed_dataframe._append({'Question': question, 'Translated question': eng_question, 'GPT Output': gpt_output, 'Retrieved Documents': documents, 'Sources': sources}, ignore_index=True)

print(processed_dataframe)


# Create a new sheet
writer = pd.ExcelWriter(excel_save_path, engine='openpyxl', mode='a', if_sheet_exists='replace')

# Assuming that df is the DataFrame you want to write to the new sheet
processed_dataframe.to_excel(writer, sheet_name='Processed Questions', index=False)

# Save the changes
writer.close()

