import asyncio
import json
import uuid
from datetime import datetime, timedelta, timezone
from random import sample
from typing import Any, AsyncIterator, Iterable, List, Optional, Set, Tuple, TypeAlias
import os
import re

from byoeb.application_logger.azure_app_insights import AppInsightsLogHandler
from byoeb.background_jobs.did_you_know.config import bot_config
from byoeb.models.dyk import DykLanguageEntry, DykRecord
from byoeb.repositories.dyk_repository import DykRepository
from byoeb.repositories.user_repository import UserRepository
from byoeb.constants.user_enums import LanguageCode
from byoeb.repositories.repository_factory import get_repository_factory
from byoeb.services.channel.base import BaseChannelService
from byoeb.services.chat import constants
from byoeb_core.models.byoeb.message_context import ByoebMessageContext, MessageContext, MessageTypes
from byoeb_core.models.byoeb.user import User
from byoeb_integrations.channel.whatsapp.meta.async_whatsapp_client import StatusCode

DykBatch: TypeAlias = Iterable[Tuple[User, Set[str]]]

def clean_template_param(text: str) -> str:
    """Make template parameter safe for WhatsApp: no newlines/tabs, no 4+ spaces."""
    # Replace newlines/tabs with single space
    text = re.sub(r"[\r\n\t]+", " ", text)
    # Collapse multiple spaces to single
    text = re.sub(r"\s{2,}", " ", text)
    return text.strip()

async def pick_candidates(dyk_repo: DykRepository, user_repo: UserRepository, langs: Iterable[LanguageCode], user_types: List[str], batch_size: int) -> AsyncIterator[DykBatch]:
    """
    Does a buffered fetch operation on users collection and get their set of sent DYK ids.
    Collects only users who meet all the following criteria:
    - the user is an asha user (prod only)
    - the user is a test user (staging only)
    - the user has a language available in `langs`
    - the user has no 'pending' DYKs
    """
    select_test_only = os.getenv("TEST_USERS_ONLY", "false").lower() == "true"
    if len(user_types) > 0:
        if select_test_only and hasattr(user_repo, "find_test_users_by_types"):
            potential_candidates = user_repo.find_test_users_by_types(user_types)
        else:
            potential_candidates = user_repo.find_users_by_types(user_types)
    else:
        if select_test_only:
            run_logger.debug(f"{pick_candidates.__name__}: TEST_USERS_ONLY enabled - selecting test users")
            potential_candidates = user_repo.find_test_users()
        else:
            run_logger.debug(f"{pick_candidates.__name__}: no user_types provided - selecting all users")
            potential_candidates = user_repo.find_all_users()
    
    filtern_user_ids = set()
    async for record in dyk_repo.find_pending_of_langs(langs):
        filtern_user_ids.add(record.user_id)
    buffer: List[User] = []
    async for doc in potential_candidates:
        user = User(**doc["User"])
        if user.user_id in filtern_user_ids:
            continue
        buffer.append(user)
        if len(buffer) == batch_size:
            yield buffer
            buffer = []
    if buffer:
        yield buffer


async def queue(dyk_repo: DykRepository, candidates: DykBatch) -> Tuple[str, int, int]:
    """
    Select DYK entries for the provided candidates directly through the repository.
    """
    batch_id = uuid.uuid4().hex
    n_queued = 0
    n_exhausted = 0
    queued_client_ops = []

    for user in candidates:
        if user.user_language is None:
            continue
        try:
            lang = LanguageCode(user.user_language)
        except ValueError:
            continue

        dyk_id = await dyk_repo.select_next(str(user.user_id), lang)
        if not dyk_id:
            send_logger.warning("User %s exhausted for language %s", user.user_id, lang, extra={AppInsightsLogHandler.DETAILS: {
                "context": queue.__name__,
                "user_id": user.user_id,
                "user_phone_number": user.phone_number_id,
                "dyk_lang": lang.value
            }})
            n_exhausted += 1
            continue

        queued_client_ops.append(DykRecord(
            id="",
            dyk_id=dyk_id,
            dyk_lang=lang,
            user_id=str(user.user_id),
            time=datetime.now(),
            status="pending",
            batch_id=batch_id
        ))
        send_logger.info("[batch-%s] User %s is assigned DYK %s", batch_id, user.user_id, dyk_id, extra={AppInsightsLogHandler.DETAILS: {
            "context": queue.__name__,
            "dyk_id": str(dyk_id),
            "user_id": user.user_id,
            "batch_id": batch_id,
            "user_phone_number": user.phone_number_id
        }})
        n_queued += 1

    if len(queued_client_ops) > 0:
        await dyk_repo.insert(queued_client_ops)
    return batch_id, n_queued, n_exhausted


def message_simple(user: User, record: DykRecord, entry: DykLanguageEntry, ts: datetime) -> ByoebMessageContext:
    return ByoebMessageContext(
        channel_type="whatsapp",
        message_category="did_you_know",
        user=user,
        message_context=MessageContext(
            message_id=f"did-you-know-{record.id}",
            message_source_text=None,
            message_english_text=None,
            media_info=None,
            message_type=MessageTypes.TEMPLATE_TEXT.value,
            additional_info={
                constants.TEMPLATE_NAME: "did_you_know_v2",
                constants.TEMPLATE_LANGUAGE: record.dyk_lang.value,
                constants.TEMPLATE_PARAMETERS: [clean_template_param(entry.fact)]
            }
        ),
        reply_context=None,
        cross_conversation_id=None,
        cross_conversation_context=None,
        incoming_timestamp=int(ts.timestamp()),
        outgoing_timestamp=int(ts.timestamp())
    )


def message_with_related_questions(user: User, record: DykRecord, entry: DykLanguageEntry, ts: datetime) -> ByoebMessageContext:
    button_titles = sample(entry.related_questions, k=min(len(entry.related_questions), 3)) if entry.related_questions else []
    additional_info = {constants.BUTTON_TITLES: button_titles} if button_titles else None
    message_type = MessageTypes.INTERACTIVE_BUTTON.value if button_titles else MessageTypes.REGULAR_TEXT.value
    return ByoebMessageContext(
        channel_type="whatsapp",
        message_category="did_you_know",
        user=user,
        message_context=MessageContext(
            message_id=f"did-you-know-{record.id}",
            message_type=message_type,
            message_source_text=LANGUAGE_TEMPLATES[record.dyk_lang].replace("{message}", entry.fact),
            message_english_text=LANGUAGE_TEMPLATES[LanguageCode.ENGLISH].replace("{message}", entry.fact),
            media_info=None,
            additional_info=additional_info,
        ),
        reply_context=None,
        cross_conversation_id=None,
        cross_conversation_context=None,
        incoming_timestamp=int(ts.timestamp()),
        outgoing_timestamp=int(ts.timestamp())
    )


async def dispatch(dyk_repo: DykRepository, user_repo: UserRepository, channel: BaseChannelService, batch_id: str, langs: Iterable[LanguageCode]) -> Tuple[int, int]:
    """ Dispatches queued DYK messages to channel. Returns number of successful and unsuccessful operations. """
    pending = [p async for p in dyk_repo.find_pending_of_batches(langs, [batch_id])]
    users: dict[str, Optional[User]] = {p.user_id: None for p in pending}
    async for user_doc in user_repo.find_users_by_ids(list(users.keys())):
        user = User(**user_doc["User"])
        users[str(user.user_id)] = user

    ts = datetime.now(timezone.utc)
    wa_engagement_window = timedelta(days=1)
    n_success = 0
    n_failure = 0

    aborted = []
    completed = []
    try:
        for record in pending:
            user = users[record.user_id]
            if not user:
                send_logger.warning("User %s not found", record.user_id, extra={AppInsightsLogHandler.DETAILS: {
                    "context": dispatch.__name__,
                    "dyk_id": str(record.dyk_id),
                    "user_id": record.user_id,
                    "batch_id": batch_id
                }})
                aborted.append(record.id)
                continue

            phone_number = user.phone_number_id
            entry = await dyk_repo.find(record.dyk_id)
            if entry is None or record.dyk_lang not in entry.languages:
                continue

            lang_entry = entry.languages[record.dyk_lang]

            if user.activity_timestamp is None or ts - user.activity_timestamp > wa_engagement_window:
                byoeb_message = message_simple(user, record, lang_entry, ts)
            else:
                byoeb_message = message_with_related_questions(user, record, lang_entry, ts)

            requests = channel.prepare_requests(byoeb_message)
            if not requests:
                send_logger.error(
                    "Failed to prepare a request message (template may be missing or additional_info keys wrong)",
                    extra={AppInsightsLogHandler.DETAILS: {
                    "context": dispatch.__name__,
                    "dyk_id": str(record.dyk_id),
                    "user_id": record.user_id,
                    "batch_id": batch_id,
                    "user_phone_number": phone_number
                }})
                n_failure += 1
                continue

            # TODO: we should probably batch `requests` here so we call send_requests() sparingly...
            responses, message_ids = await channel.send_requests(requests)
            assert len(responses) > 0
            failed = False
            for response in responses:
                if int(response.response_status.status) != StatusCode.SUCCESS.value:
                    send_logger.error(response.response_status.error, extra={AppInsightsLogHandler.DETAILS: {
                        "context": dispatch.__name__,
                        "dyk_id": str(record.dyk_id),
                        "user_id": record.user_id,
                        "batch_id": batch_id,
                        "user_phone_number": phone_number,
                        "channel_response_code": response.response_status.status
                    }})
                    n_failure += 1
                    failed = True

            if not failed:
                send_logger.info("Sent DYK %s to user %s", record.dyk_id, record.user_id, extra={AppInsightsLogHandler.DETAILS: {
                    "context": dispatch.__name__,
                    "dyk_id": str(record.dyk_id),
                    "user_id": record.user_id,
                    "batch_id": batch_id,
                    "user_phone_number": phone_number,
                    "channel_message_ids": json.dumps(message_ids)
                }})
                completed.append(record.id)
                n_success += 1
    finally:
        await dyk_repo.update_status(aborted, "aborted")
        await dyk_repo.update_status(completed, "completed")
    return n_success, n_failure


async def main(user_types: List[str], batch_size: int, channel: BaseChannelService) -> None:
    factory = await get_repository_factory()
    dyk_repo = await factory.get_dyk_repository()
    user_repo = await factory.get_user_repository()
    active_langs = await dyk_repo.find_available_languages()

    # sync (...with runtime. delete pending records with unknown langs, unknown dyk ids)
    synced = await dyk_repo.synchronize()
    run_logger.info("Synced jobs: %d", synced)  # pending messages that were discarded (because they no longer reference a DYK message)

    # schedule (pick candidates in batches and assign them dyk ids)
    async for batch in pick_candidates(dyk_repo, user_repo, active_langs, user_types, batch_size):
        batch_id, queued, exhausted = await queue(dyk_repo, batch)
        run_logger.info("[batch-%s] Queued jobs: %d", batch_id, queued, extra={AppInsightsLogHandler.DETAILS: {"batch_id": batch_id}})  # messages that were added to the dispatch queue
        run_logger.info("[batch-%s] Exhausted jobs: %d", batch_id, exhausted, extra={AppInsightsLogHandler.DETAILS: {"batch_id": batch_id}})  # users who could not be sent a DYK message (because they have received every DYK message)

    # dispatch (...to channel. pick a batch of candidates and send them their assigned dyks)
    batch_ids = [b async for b in dyk_repo.find_pending_batch_ids()]
    retries = 0
    while True:
        if retries > 0:
            run_logger.warning("Retrying dispatch job... %d / %d", retries + 1, N_RETRIES)

        failed_batches = []
        for batch_id in batch_ids:
            success, fail = await dispatch(dyk_repo, user_repo, channel, batch_id, active_langs)
            run_logger.info("[batch-%s] Dispatched jobs: %d succeeded, %d failed", batch_id, success, fail, extra={AppInsightsLogHandler.DETAILS: {"batch_id": batch_id}})  # messages that were sent to channel (includes messages that were just queued)
            if fail > 0:
                failed_batches.append(batch_id)

        if len(failed_batches) == 0:
            break
        batch_ids = failed_batches
        retries += 1
        if retries == N_RETRIES:
            run_logger.error("Max retries exceeded. Exiting.")
            break
        await asyncio.sleep(2.5)


run_logger = AppInsightsLogHandler.getLogger("dyk_run")
send_logger = AppInsightsLogHandler.getLogger("dyk_send")

user_types_to_send = bot_config["user_types_to_send"]
# WhatsApp-configured templates now drive DYK; keep a simple language list for CSV parsing.
N_RETRIES = 5  # number of times to retry dispatch()ing to channel in the event of failure

LANGUAGE_TEMPLATES: dict[LanguageCode, str] = {LanguageCode(lang["language"]): "\n".join(lang["template"]) for lang in bot_config["languages"]}
LANGUAGE_TEMPLATES.update({code: "{message}" for code in LanguageCode if code not in LANGUAGE_TEMPLATES})

# Wrapper function for scheduler to call without arguments
async def run():
    """Wrapper function that loads config and calls main() - used by scheduler"""
    from byoeb.chat_app.configuration.dependency_setup import channel_client_factory
    from byoeb.services.channel.whatsapp import WhatsAppService

    await main(user_types_to_send, 2048, WhatsAppService(channel_client_factory))
