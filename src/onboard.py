from database import BotConvDB, AppLogger
import json
import os
from azure_language_tools import translator
from messenger import WhatsappMessenger
from datetime import datetime


def onboard_template(config: dict, app_logger: AppLogger, data_row: dict, messenger: WhatsappMessenger, bot_conv_db: BotConvDB) -> None:
    print("Onboarding template")


    user_type = data_row.get('user_type', None)

    if user_type == 'Asha':
        lang = data_row.get('user_language', 'hi')
        sent_msg_id = messenger.send_template(
            data_row['whatsapp_id'],
            'asha_onboarding',
            lang,
        )

    elif user_type == 'ANM':
        lang = data_row.get('user_language', 'hi')
        sent_msg_id = messenger.send_template(
            data_row['whatsapp_id'],
            'anm_onboarding',
            lang,
        )

    bot_conv_db.insert_row(
        receiver_id=data_row['user_id'],
        message_type=f'{user_type}_onboarding_template',
        message_id=sent_msg_id,
        audio_message_id=None,
        message_source_lang=None,
        message_language=lang,
        message_english=None,
        reply_id=None,
        citations=None,
        message_timestamp=datetime.now(),
        transaction_message_id=None,

    )
        
    return
    

def onboard_wa_helper(
    config: dict,
    app_logger: AppLogger,
    user_row: dict,
    messenger: WhatsappMessenger,
) -> None:
    welcome_messages = json.load(
        open(
            os.path.join(os.environ['APP_PATH'], os.environ['DATA_PATH'],"onboarding/welcome_messages.json"),
        )
    )
    language_prompts = json.load(
        open(
            os.path.join(os.environ['APP_PATH'], os.environ['DATA_PATH'],"onboarding/language_prompts.json"),
        )
    )
    suggestion_questions = json.load(
        open(
            os.path.join(os.environ['APP_PATH'], os.environ['DATA_PATH'],"onboarding/suggestion_questions.json"),
        )
    )
    lang = user_row['user_language']
    if user_row['user_type'] in config["USERS"]:
        for message in welcome_messages["users"][lang]:
            messenger.send_message(user_row['whatsapp_id'], message)
        audio_file = os.path.join(os.environ['APP_PATH'], os.environ['DATA_PATH'],f"onboarding/welcome_messages_users_{lang}.aac")
        messenger.send_audio(audio_file, user_row['whatsapp_id'])
        # messenger.send_language_poll(
        #     user_row['whatsapp_id'],
        #     language_prompts[lang],
        #     language_prompts[lang + "_title"],
        # )
        title, questions, list_title = (
            suggestion_questions[lang]["title"],
            suggestion_questions[lang]["questions"],
            suggestion_questions[lang]["list_title"],
        )
        messenger.send_suggestions(user_row['whatsapp_id'], title, list_title, questions)
        messenger.send_video_helper('1168012307647784', user_row['whatsapp_id']) #ASHA video (TODO: replace later)
        return

    if user_row['user_type'] in config["EXPERTS"]:
        for message in welcome_messages["experts"][lang]:
            messenger.send_message(user_row['whatsapp_id'], message)
        audio_file = os.path.join(os.environ['APP_PATH'], os.environ['DATA_PATH'],f"onboarding/welcome_messages_experts_{lang}.aac")
        messenger.send_audio(audio_file, user_row['whatsapp_id'])
        messenger.send_video_helper('2319023238489534', user_row['whatsapp_id']) #ANM video (TODO: replace later)
        # messenger.send_language_poll(
        #     user_row['whatsapp_id'],
        #     language_prompts[lang],
        #     language_prompts[lang + "_title"],
        # )
        return

    return