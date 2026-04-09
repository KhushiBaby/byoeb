from datetime import datetime, timezone
import hashlib
import json
import logging
import os
import re
from typing import Any, Iterable, List, TypeVar
from urllib.parse import unquote

from byoeb.constants.onboarding_text import ONBOARD_WELCOME_MESSAGE_DICT

logger = logging.getLogger(__name__)
from byoeb.constants.user_enums import LanguageCode
from byoeb_core.models.byoeb.user import PhoneNumberId
from fastmcp.server.dependencies import get_http_request
from pydantic import TypeAdapter, ValidationError

logger = logging.getLogger(__name__)


def mask_phone(phone: str, visible_tail: int = 4) -> str:
    """Mask phone number for logs/telemetry: show only last visible_tail chars, rest as asterisks."""
    if not phone or not isinstance(phone, str):
        return "****"
    s = str(phone).strip()
    if len(s) <= visible_tail:
        return "*" * len(s) if s else "****"
    return "*" * (len(s) - visible_tail) + s[-visible_tail:]


def mask_message_preview(text: str, max_visible: int = 0) -> str:
    """Redact message content for logs/telemetry. Returns [redacted] or [len=N] to avoid PII."""
    if not text or not isinstance(text, str):
        return "[redacted]"
    n = len(text.strip())
    if max_visible <= 0:
        return "[len=%d]" % n
    return "[len=%d]" % n


def get_git_root_path():
    current_dir = os.path.abspath(__file__)
    try:
        while current_dir != os.path.dirname(current_dir):  # Stop at the filesystem root
            if os.path.isdir(os.path.join(current_dir, ".github")):
                return current_dir
            current_dir = os.path.dirname(current_dir)
        return current_dir
    except Exception as e:
        logger.error("Error determining git root: %s", str(e), exc_info=True)
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
    # Safety check: if text is None or empty, return False
    if not text or not isinstance(text, str):
        logger.debug("[is_onboard] Invalid text input: %s (type: %s)", text, type(text).__name__)
        return False
    
    original_text = text
    text = unquote(text)  # "%20%" -> " "
    
    # Helper function to normalize whitespace (multiple spaces -> single space)
    def normalize_whitespace(s):
        if not s:
            return ""
        return re.sub(r'\s+', ' ', s).strip()
    
    # Helper function to check onboarding in a specific language
    def check_language(check_lang: str) -> bool:
        if check_lang not in ONBOARD_WELCOME_MESSAGE_DICT:
            return False
        
        # For English, normalize to lowercase and replace hyphens
        # For other languages (Hindi, Marathi, Telugu), don't use .lower() as it may not work correctly
        if check_lang == LanguageCode.ENGLISH.value:
            # Normalize text: lowercase, replace hyphens with spaces, normalize whitespace
            text_normalized = normalize_whitespace(text.lower().replace("-", " "))
            logger.debug("[is_onboard] English text normalized: '%s' -> '%s'", text, text_normalized)
        else:
            # For non-English, just replace hyphens and normalize whitespace, keep original case
            text_normalized = normalize_whitespace(text.replace("-", " "))
            logger.debug("[is_onboard] Non-English text normalized: '%s' -> '%s'", text, text_normalized)
        
        # Check if any phrase exists in text (case-insensitive for English, exact match for others)
        is_english = check_lang == LanguageCode.ENGLISH.value
        for phrase in ONBOARD_WELCOME_MESSAGE_DICT[check_lang]:
            # Normalize phrase: replace hyphens with spaces, normalize whitespace
            # For English, also convert to lowercase
            phrase_normalized = normalize_whitespace(phrase.replace("-", " "))
            if is_english:
                phrase_normalized = phrase_normalized.lower()
            
            logger.debug("[is_onboard] Comparing %s phrase '%s' in text '%s' -> %s", check_lang, phrase, text_normalized, phrase_normalized in text_normalized)
            if phrase_normalized in text_normalized:
                logger.debug("[is_onboard] ✓ Matched %s phrase '%s' in text '%s'", check_lang, phrase, original_text)
                return True
        
        return False
    
    # If lang is None, try all languages (handles cases where user_language is not set)
    if lang is None:
        logger.debug("[is_onboard] Language is None, trying all languages")
        for available_lang in ONBOARD_WELCOME_MESSAGE_DICT.keys():
            if check_language(available_lang):
                return True
        logger.debug("[is_onboard] ✗ No match found in any language. original_text='%s'", original_text)
        return False

    # If lang is explicitly set but not supported, return False
    if lang not in ONBOARD_WELCOME_MESSAGE_DICT:
        logger.debug("[is_onboard] Language '%s' not in ONBOARD_WELCOME_MESSAGE_DICT", lang)
        return False
    
    # Check the specified language first
    if check_language(lang):
        return True
    
    # If no match in the specified language, try all other languages as a fallback
    # This handles cases where user_language is set incorrectly
    logger.debug("[is_onboard] No match in %s, trying other languages as fallback", lang)
    for available_lang in ONBOARD_WELCOME_MESSAGE_DICT.keys():
        if available_lang != lang:  # Don't recheck the same language
            if check_language(available_lang):
                return True

    logger.debug("[is_onboard] ✗ No match found in any language. lang=%s, original_text='%s'", lang, original_text)
    return False

T = TypeVar("T")
def chunked(seq: Iterable[T], size: int) -> Iterable[List[T]]:
    seq = list(seq)
    for i in range(0, len(seq), size):
        yield seq[i:i + size]

def hash_dict(d):
    return hashlib.sha256(
        json.dumps(d, sort_keys=True, separators=(',', ':'), ensure_ascii=False)
        .encode()
    ).hexdigest()


def ensure_utc_dates(obj: Any) -> Any:
    """Recursively ensure datetime values are UTC-aware so User model accepts them (e.g. from MongoDB)."""
    if isinstance(obj, datetime):
        return obj.replace(tzinfo=timezone.utc) if obj.tzinfo is None else obj
    if isinstance(obj, dict):
        return {k: ensure_utc_dates(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [ensure_utc_dates(v) for v in obj]
    return obj