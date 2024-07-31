from abc import ABC, abstractmethod
import sys
import os

sys.path.append(os.path.dirname(__file__))
src_path = os.path.join(os.environ["APP_PATH"], "src")
print(src_path)

import requests
from conversation_database import (
    ConversationDatabase,
    LongTermDatabase,
    LoggingDatabase,
)
from knowledge_base import KnowledgeBase
from datetime import datetime
import numpy as np
from azure_language_tools import translator
from utils import remove_extra_voice_files
import subprocess
from messenger.base import BaseMessenger


class WhatsappMessenger(BaseMessenger):
    def __init__(self, config, app_logger):
        self.config = config
        self.app_logger = app_logger
        
        self.users_types = self.config["USERS"]
        self.experts_types = []
        self.categories = []
        self.category_to_expert = {}
        for expert in self.config["EXPERTS"]:
            self.experts_types.append(self.config["EXPERTS"])
            self.categories.append(self.config["EXPERTS"][expert])
            self.category_to_expert[self.config["EXPERTS"][expert]] = expert


    def send_message(
        self,
        to_number: str,
        msg_body: str,
        reply_to_msg_id: str = None,
    ):
        payload = {
            "messaging_product": "whatsapp",
            "to": to_number,
            "text": {"body": msg_body},
        }

        if reply_to_msg_id is not None:
            payload["context"] = {"message_id": reply_to_msg_id}

        headers = {
            "Authorization": "Bearer " + os.environ["WHATSAPP_TOKEN"].strip(),
            "Content-Type": "application/json",
        }

        url = (
            "https://graph.facebook.com/v12.0/"
            + os.environ["PHONE_NUMBER_ID"]
            + "/messages"
        )
        msg_output = requests.post(url, json=payload, headers=headers)

        print("Message output: ", msg_output.json())
        msg_id = msg_output.json()["messages"][0]["id"]

        self.app_logger.add_log(
            event_name="send_message",
            sender_id="bot",
            receiver_id=to_number,
            message_id=msg_id,
            details={"text": msg_body, "reply_to": reply_to_msg_id},
        )

        return msg_id

    def send_reaction(
        self,
        to_number: str,
        reply_to_msg_id: str = None,
        emoji: str = "👍",
    ):
        payload = {
            "messaging_product": "whatsapp",
            "to": to_number,
            "type": "reaction",
            "reaction": {"message_id": reply_to_msg_id, "emoji": emoji},
        }

        headers = {
            "Authorization": "Bearer " + os.environ["WHATSAPP_TOKEN"].strip(),
            "Content-Type": "application/json",
        }
        url = (
            "https://graph.facebook.com/v17.0/"
            + os.environ["PHONE_NUMBER_ID"]
            + "/messages"
        )

        msg_output = requests.post(url, json=payload, headers=headers)
        msg_id = msg_output.json()["messages"][0]["id"]

        self.app_logger.add_log(
            event_name="send_reaction",
            sender_id="bot",
            receiver_id=to_number,
            message_id=msg_id,
            details={"emoji": emoji, "reply_to": reply_to_msg_id},
        )

        return

    def send_poll(
        self,
        to_number: str,
        poll_string: str,
        reply_to_msg_id: str = None,
        poll_id: str = None,
        send_to: str = None,
    ):
        payload = {
            "messaging_product": "whatsapp",
            "to": to_number,
            "type": "interactive",
            "interactive": {
                "type": "button",
                "body": {"text": poll_string},
                "action": {
                    "buttons": [
                        {
                            "type": "reply",
                            "reply": {"id": poll_id + "_YES", "title": "Yes"},
                        },
                        {
                            "type": "reply",
                            "reply": {"id": poll_id + "_NO", "title": "No"},
                        }
                    ]
                },
            },
        }

        if reply_to_msg_id is not None:
            payload["context"] = {"message_id": reply_to_msg_id}

        headers = {
            "Authorization": "Bearer " + os.environ["WHATSAPP_TOKEN"].strip(),
            "Content-Type": "application/json",
        }
        url = (
            "https://graph.facebook.com/v17.0/"
            + os.environ["PHONE_NUMBER_ID"]
            + "/messages"
        )

        msg_output = requests.post(url, json=payload, headers=headers)

        try:
            msg_id = msg_output.json()["messages"][0]["id"]
        except KeyError:
            print(msg_output.json())
            return None
        self.app_logger.add_log(
            event_name="send_poll",
            sender_id="bot",
            receiver_id=to_number,
            message_id=msg_id,
            details={
                "text": poll_string,
                "reply_to": reply_to_msg_id,
                "options": ["Yes", "No", "Send to " + send_to],
            },
        )

        self.send_reaction(to_number, msg_id, "📝")

        return msg_id
    
    def send_feedback_poll(
        self,
        to_number: str,
        poll_string: str,
        reply_to_msg_id: str = None,
        buttons: list = ["Yes", "No"],
    ):
        
        payload = {
            "messaging_product": "whatsapp",
            "to": to_number,
            "type": "interactive",
            "interactive": {
                "type": "button",
                "body": {"text": poll_string},
                "action": {
                    "buttons": [
                        {
                            "type": "reply",
                            "reply": {"id": "feedback_poll" + "_YES", "title": buttons[0]},
                        },
                        {
                            "type": "reply",
                            "reply": {"id": "feedback_poll" + "_NO", "title": buttons[1]},
                        }
                    ]
                },
            },
        }

        if reply_to_msg_id is not None:
            payload["context"] = {"message_id": reply_to_msg_id}

        headers = {
            "Authorization": "Bearer " + os.environ["WHATSAPP_TOKEN"].strip(),
            "Content-Type": "application/json",
        }
        url = (
            "https://graph.facebook.com/v17.0/"
            + os.environ["PHONE_NUMBER_ID"]
            + "/messages"
        )

        msg_output = requests.post(url, json=payload, headers=headers)

        try:
            msg_id = msg_output.json()["messages"][0]["id"]
        except KeyError:
            print(msg_output.json())
            return None
        self.app_logger.add_log(
            event_name="send_poll",
            sender_id="bot",
            receiver_id=to_number,
            message_id=msg_id,
            details={
                "text": poll_string,
                "reply_to": reply_to_msg_id,
                "options": buttons,
            },
        )
        return msg_id
        


    def send_language_poll(
        self,
        to_number: str,
        poll_string: str,
        title: str,
    ):
        payload = {
            "messaging_product": "whatsapp",
            "to": to_number,
            "type": "interactive",
            "interactive": {
                "type": "list",
                "body": {"text": poll_string},
                "action": {
                    "button": title,
                    "sections": [
                        {
                            "title": "Language Selection",
                            "rows": [
                                {"id": "LANG_ENG", "title": "English"},
                                {"id": "LANG_HIN", "title": "हिंदी"},
                                # {"id": "LANG_KNA", "title": "ಕನ್ನಡ"},
                                # {"id": "LANG_TAM", "title": "தமிழ்"},
                                # {"id": "LANG_TEL", "title": "తెలుగు"},
                            ],
                        }
                    ],
                },
            },
        }

        headers = {
            "Authorization": "Bearer " + os.environ["WHATSAPP_TOKEN"].strip(),
            "Content-Type": "application/json",
        }
        url = (
            "https://graph.facebook.com/v17.0/"
            + os.environ["PHONE_NUMBER_ID"]
            + "/messages"
        )

        msg_output = requests.post(url, json=payload, headers=headers)
        print(msg_output.json())
        msg_id = msg_output.json()["messages"][0]["id"]
        self.app_logger.add_log(
            event_name="send_language_poll",
            sender_id="bot",
            receiver_id=to_number,
            message_id=msg_id,
            details={
                "text": poll_string,
                "reply_to": None,
                "options": ["English", "हिंदी", "ಕನ್ನಡ", "தமிழ்", "తెలుగు"],
            },
        )

        return msg_id

    def send_suggestions(
        self,
        to_number: str,
        text_poll: str = None,
        list_title: str = None,
        questions: list = None,
    ):
        if questions is None or questions == []:
            return
        final_questions_list = []

        for i, question in enumerate(questions):
            if len(question) > 72:
                question = question[:69] + "..."
            final_questions_list.append(
                {"id": "QUESTION_" + str(i + 1), "title": " ", "description": question}
            )

        payload = {
            "messaging_product": "whatsapp",
            "to": to_number,
            "type": "interactive",
            "interactive": {
                "type": "list",
                "body": {"text": text_poll},
                "action": {
                    "button": list_title,
                    "sections": [
                        {"title": list_title, "rows": final_questions_list},
                    ],
                },
            },
        }

        headers = {
            "Authorization": "Bearer " + os.environ["WHATSAPP_TOKEN"].strip(),
            "Content-Type": "application/json",
        }
        url = (
            "https://graph.facebook.com/v17.0/"
            + os.environ["PHONE_NUMBER_ID"]
            + "/messages"
        )

        msg_output = requests.post(url, json=payload, headers=headers)
        print(msg_output.json())
        msg_id = msg_output.json()["messages"][0]["id"]

        self.app_logger.add_log(
            event_name="send_suggestions",
            sender_id="bot",
            receiver_id=to_number,
            message_id=msg_id,
            details={"text": text_poll, "suggestions": questions},
        )

        return msg_id

    def send_template(
        self,
        to_number: str,
        template_name: str,
        language: str,
        reply_to_msg_id: str = None,
    ):
        payload = {
            "messaging_product": "whatsapp",
            "to": to_number,
            "type": "template",
            "template": {
                "name": template_name,
                "language": {
                    "code": language,
                },
            },
        }

        if reply_to_msg_id is not None:
            payload["context"] = {"message_id": reply_to_msg_id}

        headers = {
            "Authorization": "Bearer " + os.environ["WHATSAPP_TOKEN"].strip(),
            "Content-Type": "application/json",
        }

        url = (
            "https://graph.facebook.com/v17.0/"
            + os.environ["PHONE_NUMBER_ID"]
            + "/messages"
        )
        msg_output = requests.post(url, json=payload, headers=headers)

        print("Message output: ", msg_output.json())
        msg_id = msg_output.json()["messages"][0]["id"]

        self.app_logger.add_log(
            event_name="send_message",
            sender_id="bot",
            receiver_id=to_number,
            message_id=msg_id,
            details={"text": template_name, "reply_to": reply_to_msg_id},
        )

        return msg_id

    def send_correction_poll_expert(
        self,
        row_lt,
        row_query,
        escalation: bool = False,
    ) -> None:
        """Sends a poll asking if the bot's response was correct

        Args:
            database (ConversationDatabase): the database
            db_id (str): the database ID
        """


        query_type = row_query["query_type"]
        expert_type = self.category_to_expert[query_type]
        user_secondary_id = self.user_relation_db.get_from_user_id(row_lt['user_id'], expert_type)['user_id']
        expert_row_lt = self.user_db.get_from_user_id(user_secondary_id)
        

        user_type = row_lt["user_type"]
        
        poll_string = f"Was the bot's answer correct and complete?"

        citations = row_query["citations"]
        try:
            split_citations = citations.split("\n")
            split_citations = np.unique(
                np.array(
                    [
                        citation.replace("_", " ").replace("  ", " ").strip()
                        for citation in split_citations
                    ]
                )
            )
            final_citations = ", ".join([citation for citation in split_citations])
        except:
            final_citations = "No citations found."

        expert = self.category_to_expert[row_query['query_type']]
        if escalation is False:
            receiver = expert_row_lt["whatsapp_id"]
            forward_to = expert
        else:
            receiver = self.config["ESCALATION"][expert]['whatsapp_id']
            forward_to = expert


        

        poll_text = f'*Query*: "{row_query["query"]}" \n*Bot\'s Response*: {row_query["llm_response"].strip()} \n\n*User*: {user_type} \n*Citations*: {final_citations.strip()}. \n\n{poll_string}'
        message_id = self.send_poll(
            receiver, poll_text, poll_id="POLL_PRIMARY", send_to=forward_to
        )

        # if escalation is False:
        #     database.add_poll_primary_id(db_id, message_id)
        #     database.add_poll_escalated_id(db_id, None)
        # else:
        #     database.add_poll_escalated_id(db_id, message_id)
        #     receiver_name = self.config["ESCALATION"][expert]['name']
        #     primanry_notif = row_lt[expert + "_whatsapp_id"]
        #     self.send_message(
        #         primanry_notif,
        #         "Escalating it to " + receiver_name,
        #         reply_to_msg_id=row["poll_primary_id"],
        #     )



        return message_id

    def receive_correction_poll_expert(
        self,
        database: ConversationDatabase,
        long_term_db: LongTermDatabase,
        msg_object: dict,
        azure_translate: translator,
    ) -> None:
        """receive the correction poll (yes or no)
        Args:
            database (ConversationDatabase): the database
            msg_object (dict): the message object
        """

        answer = msg_object["interactive"]["button_reply"]["title"]
        context_id = msg_object["context"]["id"]

        self.app_logger.add_log(
            event_name="receive_poll",
            sender_id=msg_object["from"],
            receiver_id="bot",
            message_id=msg_object["id"],
            details={"answer": answer, "reply_to": context_id},
        )

        row_list_primary = database.find_with_poll_primary_id(context_id)
        row_list_escalated = database.find_with_poll_escalated_id(context_id)
        if len(row_list_primary) == 0 and len(row_list_escalated) == 0:
            self.send_message(
                msg_object["from"],
                f"Please reply to the query that you are trying to answer.",
                msg_object["id"],
            )
            return
        print("Row List 1", row_list_primary)
        print("Row List 2", row_list_escalated)

        row = row_list_primary[0] if row_list_primary else row_list_escalated[0]
        row_lt = long_term_db.get_rows(row["user_id"], "user_id")[0]
        ans_num = msg_object["from"]


        if row["is_correct"] is not None:
            ans_num = row["answered_by"]
            if ans_num == msg_object["from"]:
                self.send_message(
                    msg_object["from"],
                    "You have already responded to this query before.",
                    context_id,
                )
                return
            else:
                expert = self.category_to_expert[row['query_type']]
                if ans_num == row_lt[expert+"_whatsapp_id"]:
                    ans_name = row_lt[expert+"_name"]
                else:
                    ans_name = self.config["ESCALATION"][expert]['name']
                self.send_message(
                    msg_object["from"],
                    ans_name + " has already responded to this question before.",
                    context_id,
                )
                return

        row_lt = long_term_db.get_rows(row['user_id'], 'user_id')[0]
        expert = self.category_to_expert[row['query_type']]
        lang = row_lt[row["user_type"] + "_language"]
            

        if answer == "Yes":
            database.add_is_correct(row["_id"], True, msg_object["from"])
            self.send_message(
                msg_object["from"], "Noted, thank you for the response.", context_id
            )
            
            self.send_reaction(
                row_lt[row["user_type"] + "_whatsapp_id"], row["response_message_id"], "\u2705"
            )
            if row["audio_response_message_id"]:
                self.send_reaction(
                    row_lt[row["user_type"] + "_whatsapp_id"],
                    row["audio_response_message_id"],
                    "\u2705",
                )
            text = f"This response has been verified by the {expert}."
            text_translated = azure_translate.translate_text(
                text, "en", lang, self.app_logger
            )
            self.send_message(
                row_lt[row["user_type"] + "_whatsapp_id"],
                text_translated,
                row["response_message_id"],
            )

            self.send_reaction(row_lt[expert+"_whatsapp_id"], row["poll_primary_id"], "\u2705")
            if row["poll_escalated_id"]:
                self.send_reaction(
                    self.config["ESCALATION"][expert]['whatsapp_id'],
                    row["poll_escalated_id"],
                    "\u2705",
                )
            
        elif answer == "No":
            self.send_reaction(
                row_lt[row["user_type"] + "_whatsapp_id"], row["response_message_id"], "\u274C"
            )
            text = f"This answer is invalid. Please wait for the correct response from the {expert}."
            text_translated = azure_translate.translate_text(
                text, "en", lang, self.app_logger
            )
            self.send_message(
                row_lt[row["user_type"] + "_whatsapp_id"],
                text_translated,
                row["response_message_id"],
            )
            if row["audio_response_message_id"]:
                self.send_reaction(
                    row_lt[row["user_type"] + "_whatsapp_id"],
                    row["audio_response_message_id"],
                    "\u274C",
                )

            correction_msg_id = self.send_message(
                msg_object["from"],
                "Please reply with a correction to the query that you are trying to fix.",
                context_id,
            )
            database.add_is_correct(row["_id"], False, msg_object["from"])

        return

    def get_correction_from_expert(
        self,
        database: ConversationDatabase,
        msg_object: dict,
        azure_translate: translator,
        long_term_db: LongTermDatabase,
        knowledge_base: KnowledgeBase,
    ) -> None:
        """Get a correction from the user"""
        
        msg_body = msg_object["text"]["body"]
        from_number = msg_object["from"]
        try:
            msg_context = msg_object["context"]["id"]
            list_msg_ids = database.find_db_id_with_message_id(msg_context)
            print(msg_context, list_msg_ids)
        except:
            self.send_message(
                msg_object["from"],
                f"Please reply to the query you want to fix.",
                msg_object["id"],
            )
            return

        self.app_logger.add_log(
            event_name="received_correction",
            sender_id=from_number,
            receiver_id="bot",
            message_id=msg_object["id"],
            details={"text": msg_body, "reply_to": msg_context},
        )
        if len(list_msg_ids) == 0:
            self.send_message(
                msg_object["from"],
                f"Please reply to the query you want to fix.",
                msg_object["id"],
            )
            return

        top_row = list_msg_ids[0]

        if top_row["correction"] is not None:
            self.send_message(
                msg_object["from"],
                f"You have already sent a correction for this query.",
                msg_object["id"],
            )
            return
        
        if top_row['answered_by'] != msg_object['from']:
            self.send_message(
                msg_object["from"],
                f"This query has already been answered.",
                msg_object["id"],
            )
            return

        db_id = top_row["_id"]
        database.add_correction(db_id, msg_body, msg_object["id"], datetime.now())
        row = database.get_row_with_id(db_id)
        row_lt = long_term_db.get_rows(row['user_id'], 'user_id')[0]

        database.add_is_correct(db_id, False, msg_object["from"])

        # remove inverted commas from the output in beggining and end

        gpt_output = knowledge_base.generate_correction(database, db_id, self.app_logger)
        gpt_output = gpt_output.strip('"')

        
        source_language = row_lt[row["user_type"] + "_language"]

        if row["query_message_type"] == "audio":
            corrected_audio_loc = "corrected_audio.wav"
            remove_extra_voice_files(
                corrected_audio_loc, corrected_audio_loc[:-3] + ".aac"
            )
            gpt_output_source = azure_translate.text_translate_speech(
                gpt_output, source_language + "-IN", corrected_audio_loc, self.app_logger
            )

            updated_msg_id = self.send_message(
                row_lt[row["user_type"] + "_whatsapp_id"],
                gpt_output_source,
                row["query_message_id"],
            )

            subprocess.run(
                [
                    "ffmpeg",
                    "-i",
                    corrected_audio_loc,
                    "-codec:a",
                    "aac",
                    corrected_audio_loc[:-3] + ".aac",
                ],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            updated_audio_msg_id = self.send_audio(
                corrected_audio_loc[:-3] + ".aac",
                row_lt[row["user_type"] + "_whatsapp_id"],
                row["query_message_id"],
            )
            remove_extra_voice_files(
                corrected_audio_loc, corrected_audio_loc[:-3] + ".aac"
            )

            self.send_reaction(row_lt[row["user_type"] + "_whatsapp_id"], updated_msg_id, "\u2705")
            self.send_reaction(
                row_lt[row["user_type"] + "_whatsapp_id"], updated_audio_msg_id, "\u2705"
            )
        else:
            gpt_output_source = azure_translate.translate_text(
                gpt_output, "en", source_language, self.app_logger
            )
            updated_msg_id = self.send_message(
                row_lt[row["user_type"] + "_whatsapp_id"],
                gpt_output_source,
                row["query_message_id"],
            )

            self.send_reaction(row_lt[row["user_type"] + "_whatsapp_id"], updated_msg_id, "\u2705")

        expert = self.category_to_expert[row['query_type']]
        text = f"This response has been verified by the {expert}."
        msg_text = azure_translate.translate_text(text, "en", source_language, self.app_logger)
        self.send_message(row_lt[row["user_type"] + "_whatsapp_id"], msg_text, updated_msg_id)

        self.send_message(
            msg_object["from"], "Correction noted. Thank you.", msg_object["id"]
        )

        self.send_reaction(row_lt[expert+"_whatsapp_id"], row["poll_primary_id"], "\u2705")

        database.add_updated_response(db_id, gpt_output, updated_msg_id, datetime.now())

        if row["query_message_type"] == "audio":
            remove_extra_voice_files(
                corrected_audio_loc, corrected_audio_loc[:-3] + ".aac"
            )
        return

    def download_audio(
        self,
        msg_object: dict,
        audio_file: str,
    ) -> str:
        audio_id = msg_object["audio"]["id"]

        url = f"https://graph.facebook.com/v17.0/{audio_id}/"

        headers = {"Authorization": f"Bearer {os.environ['WHATSAPP_TOKEN'].strip()}"}

        response = requests.get(url, headers=headers)
        data = response.json()

        print("Audio output: ", data)
        data_url = data["url"]

        output_file = audio_file
        headers = {"Authorization": f"Bearer {os.environ['WHATSAPP_TOKEN'].strip()}"}
        response = requests.get(data_url, headers=headers)

        # Save the response content to a file
        with open(output_file, "wb") as file:
            file.write(response.content)

        print(f"Media file saved as {output_file}")

    def send_audio(
        self,
        audio_output_file: str,
        to_number: str,
        reply_to_msg_id: str = None,
    ) -> str:
        url = (
            "https://graph.facebook.com/v15.0/"
            + os.environ["PHONE_NUMBER_ID"]
            + "/media"
        )
        payload = {"messaging_product": "whatsapp"}
        

        if audio_output_file.endswith(".aac"):
            files = [
                ("file", (audio_output_file, open(audio_output_file, "rb"), "audio/aac"))
            ]
        elif audio_output_file.endswith(".ogg"):
            files = [
                ("file", (audio_output_file, open(audio_output_file, "rb"), "audio/ogg"))
            ]

        headers = {
            "Authorization": f"Bearer {os.environ['WHATSAPP_TOKEN'].strip()}",
        }
        response = requests.request(
            "POST", url, headers=headers, data=payload, files=files
        )

        data = response.json()
        print("Audio data: ", data)
        payload = {
            "messaging_product": "whatsapp",
            "to": to_number,
            "type": "audio",
            "audio": {"id": data["id"]},
        }

        if reply_to_msg_id is not None:
            payload["context"] = {"message_id": reply_to_msg_id}
        
        headers = {
            "Authorization": "Bearer " + os.environ["WHATSAPP_TOKEN"].strip(),
            "Content-Type": "application/json",
        }
        url = (
            "https://graph.facebook.com/v17.0/"
            + os.environ["PHONE_NUMBER_ID"]
            + "/messages"
        )

        msg_output = requests.post(url, json=payload, headers=headers)
        print("msg output: ", msg_output.json())
        msg_id = msg_output.json()["messages"][0]["id"]

        return msg_id

    def upload_video(self,
        video_file_path: str,
    ):

        url = (
            "https://graph.facebook.com/v15.0/"
            + os.environ["PHONE_NUMBER_ID"]
            + "/media"
        )
        payload = {"messaging_product": "whatsapp"}
        
        video_ext = video_file_path.split(".")[-1]
        files = [
            ("file", (video_file_path, open(video_file_path, "rb"), f"video/{video_ext}"))
        ]

        headers = {
            "Authorization": f"Bearer {os.environ['WHATSAPP_TOKEN'].strip()}",
        }
        response = requests.request(
            "POST", url, headers=headers, data=payload, files=files
        )

        data = response.json()
        return data
        

    def send_video(self,
        video_file_path: str,
        to_number: str,
        reply_to_msg_id: str = None
        ):

        data = self.upload_video(video_file_path)
        print("Video data: ", data)
        return self.send_video_helper(data["id"], to_number, reply_to_msg_id)
        
    def send_video_helper(self,
        video_id: str,
        to_number: str,
        reply_to_msg_id: str = None
    ):

        payload = {
            "messaging_product": "whatsapp",
            "to": to_number,
            "type": "video",
            "video": {"id": video_id},
        }

        if reply_to_msg_id is not None:
            payload["context"] = {"message_id": reply_to_msg_id}
        
        headers = {
            "Authorization": "Bearer " + os.environ["WHATSAPP_TOKEN"].strip(),
            "Content-Type": "application/json",
        }
        url = (
            "https://graph.facebook.com/v17.0/"
            + os.environ["PHONE_NUMBER_ID"]
            + "/messages"
        )

        msg_output = requests.post(url, json=payload, headers=headers)
        print("msg output: ", msg_output.json())
        msg_id = msg_output.json()["messages"][0]["id"]

        self.app_logger.add_log(
            event_name="send_video",
            sender_id="bot",
            receiver_id=to_number,
            message_id=msg_id,
            details={"video_id": video_id, "reply_to": reply_to_msg_id},
        )

        return msg_id

    