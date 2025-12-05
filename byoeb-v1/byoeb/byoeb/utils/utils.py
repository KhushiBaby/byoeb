import os
from typing import Iterable, List, TypeVar
from urllib.parse import unquote

from byoeb.constants.onboarding_text import ONBOARD_WELCOME_MESSAGE_DICT
from byoeb.constants.user_enums import LanguageCode
from byoeb_core.models.byoeb.user import PhoneNumberId
from fastmcp.server.dependencies import get_http_request
from pydantic import TypeAdapter, ValidationError

def get_git_root_path():
    current_dir = os.path.abspath(__file__)
    try:
        while current_dir != os.path.dirname(current_dir):  # Stop at the filesystem root
            if os.path.isdir(os.path.join(current_dir, ".github")):
                return current_dir
            current_dir = os.path.dirname(current_dir)
        return current_dir
    except Exception as e:
        print(f"Error: {str(e)}")
        return None
    
def log_to_text_file(text):
    git_root = get_git_root_path()
    file_path = os.path.join(git_root, "byoeb-v1/byoeb/log.txt")
    try:
        with open(file_path, "a", encoding="utf-8") as f:
            f.write(text + "\n")
    except FileNotFoundError:
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(text + "\n")

def mcp_get_phone_number() -> PhoneNumberId:
    request = get_http_request()
    if "phone_number" not in request.query_params:
        raise ValueError("Cannot proceed with request due to missing 'phone_number' param")

    phone_number = request.query_params["phone_number"]
    try:
        return TypeAdapter(PhoneNumberId).validate_python(phone_number)
    except ValidationError:
        raise ValueError("Cannot proceed with request due to malformed 'phone_number' param")

def is_idk(
    text: str
):
    idks = [
        "idk",
        "i don't know",
        "i do not know",
        "i don't know the answer",
        "i do not know the answer to your question"
    ]
    text = text.lower()
    return any(idk in text for idk in idks)  # Check if any phrase exists in text

def is_onboard(
    text: str,
    lang: str = LanguageCode.ENGLISH.value
):
    if lang not in ONBOARD_WELCOME_MESSAGE_DICT:
        # TODO: we should probably raise a ValueError than returning False for
        # unexpected languages.
        return False
    text = unquote(text)  # "%20%" -> " "
    text = text.lower().replace("-", " ")  # "onboard-asha" -> "onboard asha"
    return any(phrase in text for phrase in ONBOARD_WELCOME_MESSAGE_DICT[lang])  # Check if any phrase exists in text

T = TypeVar("T")
def chunked(seq: Iterable[T], size: int) -> Iterable[List[T]]:
    seq = list(seq)
    for i in range(0, len(seq), size):
        yield seq[i:i + size]