from conversation_database import LoggingDatabase
import json
import os
from azure_language_tools import translator
from messenger import WhatsappMessenger


def onboard_template(config: dict, logger: LoggingDatabase, data_row: dict, messenger: WhatsappMessenger) -> None:
    print("Onboarding template")


    user_type = data_row.get('user_type', None)

    if user_type == 'Asha':
        lang = data_row.get('user_language', 'hi')
        messenger.send_template(
            data_row['whatsapp_id'],
            'asha_onboarding',
            lang,
        )

    elif user_type == 'ANM':
        lang = data_row.get('user_language', 'hi')
        messenger.send_template(
            data_row['whatsapp_id'],
            'anm_onboarding',
            lang,
        )

    else:
        print("Invalid user type")
        
    return
    

def onboard_wa_helper(
    config: dict,
    logger: LoggingDatabase,
    user_row: dict,
) -> None:
    messenger = WhatsappMessenger(config, logger)
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
        messenger.send_language_poll(
            user_row['whatsapp_id'],
            language_prompts[lang],
            language_prompts[lang + "_title"],
        )
        title, questions, list_title = (
            suggestion_questions[lang]["title"],
            suggestion_questions[lang]["questions"],
            suggestion_questions[lang]["list_title"],
        )

        messenger.send_suggestions(user_row['whatsapp_id'], title, list_title, questions)
        return

    if user_row['user_type'] in config["EXPERTS"]:
        for message in welcome_messages["experts"][lang]:
            messenger.send_message(user_row['whatsapp_id'], message)
        audio_file = os.path.join(os.environ['APP_PATH'], os.environ['DATA_PATH'],f"onboarding/welcome_messages_experts_{lang}.aac")
        messenger.send_audio(audio_file, user_row['whatsapp_id'])
        messenger.send_language_poll(
            user_row['whatsapp_id'],
            language_prompts[lang],
            language_prompts[lang + "_title"],
        )
        return

    return