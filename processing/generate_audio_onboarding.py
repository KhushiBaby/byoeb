import os
import sys
local_path = os.path.join(os.environ['APP_PATH'], 'src')
sys.path.append(local_path)
from azure_language_tools import translator
import json
import subprocess

languages = ["en", "hi"]
roles = ["users", "experts"]

azure_translate = translator()
welcome_messages = json.load(
    open(
        os.path.join(os.environ["APP_PATH"], os.environ["DATA_PATH"], "onboarding/welcome_messages.json")
    )
)



for role in roles:
    for language in languages:
        final_message = ""
        for message in welcome_messages[role][language]:
            final_message += message + "\n\n"

        audio_path = (
            "onboarding/welcome_messages_"
            + role
            + "_"
            + language
            + ".wav"
        )
        audio_path = os.path.join(os.environ["APP_PATH"], os.environ["DATA_PATH"], audio_path)
        azure_translate.text_to_speech(final_message, language + "-IN", audio_path)

        subprocess.run(
            ["ffmpeg", "-y", "-i", audio_path, "-codec:a", "aac", audio_path[:-3] + "aac"],
            )
