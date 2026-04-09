import uuid
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, call

import pytest

from byoeb.background_jobs.did_you_know import send_dyk
from byoeb.constants.user_enums import LanguageCode
from byoeb.models.dyk import DykEntry, DykLanguageEntry, DykRecord
from byoeb.services.chat import constants
from byoeb_core.models.byoeb.message_context import MessageTypes
from byoeb_core.models.byoeb.user import User


@pytest.fixture
def aiter():
    def build(items):
        async def generator():
            for item in items:
                yield item

        return generator()

    return build


@pytest.fixture
def user_factory():
    def build(
        *,
        user_id: str,
        user_language: str = "hi",
        phone_number_id: str = "911111111111",
        user_type: str = "asha",
        activity_timestamp: datetime | None = None,
        test_user: bool = False,
    ) -> User:
        return User(
            user_id=user_id,
            user_language=user_language,
            user_type=user_type,
            phone_number_id=phone_number_id,
            activity_timestamp=activity_timestamp,
            test_user=test_user,
        )

    return build


@pytest.fixture
def user_doc_factory(user_factory):
    def build(**kwargs):
        return {"User": user_factory(**kwargs).model_dump(mode="python")}

    return build


@pytest.fixture
def dyk_record_factory():
    def build(
        *,
        record_id: str,
        user_id: str,
        dyk_lang: LanguageCode = LanguageCode.HINDI,
        dyk_id: uuid.UUID | None = None,
        batch_id: str = "batch-1",
        status: str = "pending",
    ) -> DykRecord:
        return DykRecord(
            id=record_id,
            user_id=user_id,
            dyk_id=dyk_id or uuid.uuid4(),
            dyk_lang=dyk_lang,
            time=datetime(2026, 3, 10, tzinfo=timezone.utc),
            batch_id=batch_id,
            status=status,
        )

    return build


@pytest.fixture
def dyk_entry_factory():
    def build(
        *,
        entry_id: uuid.UUID,
        fact: str = "Useful fact",
        related_questions: list[str] | None = None,
        lang: LanguageCode = LanguageCode.HINDI,
    ) -> DykEntry:
        return DykEntry(
            id=entry_id,
            languages={
                lang: DykLanguageEntry(
                    fact=fact,
                    related_questions=related_questions or [],
                )
            },
        )

    return build


@pytest.fixture
def channel_response_factory():
    def build(*, status: int = 200, error: str | None = None, message_id: str = "wamid-1"):
        return SimpleNamespace(
            response_status=SimpleNamespace(status=str(status), error=error),
            messages=[SimpleNamespace(id=message_id)],
        )

    return build


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("test_users_only", "user_types", "expected_method", "expected_args"),
    [
        ("false", ["asha"], "find_users_by_types", (["asha"],)),
        ("true", ["asha"], "find_test_users_by_types", (["asha"],)),
        ("true", [], "find_test_users", ()),
        ("false", [], "find_all_users", ()),
    ],
)
async def test_pick_candidates_selects_expected_user_source(
    monkeypatch,
    aiter,
    user_doc_factory,
    test_users_only,
    user_types,
    expected_method,
    expected_args,
):
    monkeypatch.setenv("TEST_USERS_ONLY", test_users_only)

    docs = [user_doc_factory(user_id="u-1")]
    user_repo = MagicMock()
    user_repo.find_users_by_types.return_value = aiter(docs)
    user_repo.find_test_users_by_types.return_value = aiter(docs)
    user_repo.find_test_users.return_value = aiter(docs)
    user_repo.find_all_users.return_value = aiter(docs)

    dyk_repo = MagicMock()
    dyk_repo.find_pending_of_langs.return_value = aiter([])

    batches = [
        [user.user_id for user in batch]
        async for batch in send_dyk.pick_candidates(
            dyk_repo,
            user_repo,
            [LanguageCode.HINDI],
            user_types,
            batch_size=10,
        )
    ]

    assert batches == [["u-1"]]

    methods = {
        "find_users_by_types": user_repo.find_users_by_types,
        "find_test_users_by_types": user_repo.find_test_users_by_types,
        "find_test_users": user_repo.find_test_users,
        "find_all_users": user_repo.find_all_users,
    }
    methods[expected_method].assert_called_once_with(*expected_args)
    for name, method in methods.items():
        if name != expected_method:
            method.assert_not_called()


@pytest.mark.asyncio
async def test_pick_candidates_filters_pending_users_and_yields_buffered_batches(
    monkeypatch,
    aiter,
    user_doc_factory,
    dyk_record_factory,
):
    monkeypatch.setenv("TEST_USERS_ONLY", "false")

    user_repo = MagicMock()
    user_repo.find_all_users.return_value = aiter(
        [
            user_doc_factory(user_id="u-1"),
            user_doc_factory(user_id="u-2"),
            user_doc_factory(user_id="u-3"),
            user_doc_factory(user_id="u-4"),
        ]
    )

    dyk_repo = MagicMock()
    dyk_repo.find_pending_of_langs.return_value = aiter(
        [dyk_record_factory(record_id="r-pending", user_id="u-2")]
    )

    batches = [
        [user.user_id for user in batch]
        async for batch in send_dyk.pick_candidates(
            dyk_repo,
            user_repo,
            [LanguageCode.HINDI],
            [],
            batch_size=2,
        )
    ]

    assert batches == [["u-1", "u-3"], ["u-4"]]


@pytest.mark.asyncio
async def test_queue_inserts_only_selectable_users_and_tracks_exhausted_candidates(
    monkeypatch,
    user_factory,
):
    fixed_batch_uuid = uuid.UUID("00000000-0000-0000-0000-000000000123")
    queued_dyk_id = uuid.UUID("00000000-0000-0000-0000-000000000321")

    monkeypatch.setattr(send_dyk.uuid, "uuid4", lambda: fixed_batch_uuid)

    dyk_repo = MagicMock()
    dyk_repo.select_next = AsyncMock(side_effect=[queued_dyk_id, None])
    dyk_repo.insert = AsyncMock()

    batch_id, queued, exhausted = await send_dyk.queue(
        dyk_repo,
        [
            user_factory(user_id="u-queued", user_language="hi", phone_number_id="911111111111"),
            user_factory(user_id="u-exhausted", user_language="en", phone_number_id="922222222222"),
            user_factory(user_id="u-missing-language", user_language=None, phone_number_id="933333333333"),
            user_factory(user_id="u-invalid-language", user_language="xx", phone_number_id="944444444444"),
        ],
    )

    assert batch_id == fixed_batch_uuid.hex
    assert queued == 1
    assert exhausted == 1
    assert dyk_repo.select_next.await_args_list == [
        call("u-queued", LanguageCode.HINDI),
        call("u-exhausted", LanguageCode.ENGLISH),
    ]

    dyk_repo.insert.assert_awaited_once()
    inserted_records = dyk_repo.insert.await_args.args[0]
    assert len(inserted_records) == 1
    inserted_record = inserted_records[0]
    assert inserted_record.batch_id == fixed_batch_uuid.hex
    assert inserted_record.user_id == "u-queued"
    assert inserted_record.dyk_lang == LanguageCode.HINDI
    assert inserted_record.dyk_id == queued_dyk_id
    assert inserted_record.status == "pending"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("activity_timestamp", "expected_message_type"),
    [
        (datetime(2026, 3, 8, tzinfo=timezone.utc), MessageTypes.TEMPLATE_TEXT.value),
        (datetime(2026, 3, 10, 11, 0, tzinfo=timezone.utc), MessageTypes.INTERACTIVE_BUTTON.value),
    ],
)
async def test_dispatch_chooses_message_flow_from_recent_activity(
    monkeypatch,
    aiter,
    user_doc_factory,
    dyk_record_factory,
    dyk_entry_factory,
    channel_response_factory,
    activity_timestamp,
    expected_message_type,
):
    fixed_now = datetime(2026, 3, 10, 12, 0, tzinfo=timezone.utc)

    class FixedDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            if tz is None:
                return fixed_now.replace(tzinfo=None)
            return fixed_now.astimezone(tz)

    monkeypatch.setattr(send_dyk, "datetime", FixedDatetime)

    record = dyk_record_factory(record_id="record-1", user_id="u-1", dyk_lang=LanguageCode.HINDI)
    entry = dyk_entry_factory(
        entry_id=record.dyk_id,
        fact="Fact\n   with\tspacing",
        related_questions=["Why is this useful?"],
    )

    dyk_repo = MagicMock()
    dyk_repo.find_pending_of_batches.return_value = aiter([record])
    dyk_repo.find = AsyncMock(return_value=entry)
    dyk_repo.update_status = AsyncMock()

    user_repo = MagicMock()
    user_repo.find_users_by_ids.return_value = aiter(
        [
            user_doc_factory(
                user_id="u-1",
                user_language="hi",
                phone_number_id="911111111111",
                activity_timestamp=activity_timestamp,
            )
        ]
    )

    channel = MagicMock()
    channel.prepare_requests.return_value = [{"type": "template"}]
    channel.send_requests = AsyncMock(
        return_value=([channel_response_factory(status=200)], ["wamid-1"])
    )

    success, failure = await send_dyk.dispatch(
        dyk_repo,
        user_repo,
        channel,
        "batch-1",
        [LanguageCode.HINDI],
    )

    assert (success, failure) == (1, 0)

    message = channel.prepare_requests.call_args.args[0]
    assert message.message_context.message_type == expected_message_type

    if expected_message_type == MessageTypes.TEMPLATE_TEXT.value:
        assert message.message_context.additional_info == {
            constants.TEMPLATE_NAME: "did_you_know_v2",
            constants.TEMPLATE_LANGUAGE: "hi",
            constants.TEMPLATE_PARAMETERS: ["Fact with spacing"],
        }
    else:
        assert message.message_context.additional_info == {
            constants.BUTTON_TITLES: ["Why is this useful?"]
        }
        assert "Fact\n   with\tspacing" in message.message_context.message_source_text

    assert dyk_repo.update_status.await_args_list == [
        call([], "aborted"),
        call(["record-1"], "completed"),
    ]


@pytest.mark.asyncio
async def test_dispatch_aborts_missing_users_and_completes_sent_records(
    aiter,
    user_doc_factory,
    dyk_record_factory,
    dyk_entry_factory,
    channel_response_factory,
):
    missing_record = dyk_record_factory(record_id="record-missing", user_id="u-missing")
    sent_record = dyk_record_factory(record_id="record-sent", user_id="u-sent")
    entry = dyk_entry_factory(entry_id=sent_record.dyk_id)

    dyk_repo = MagicMock()
    dyk_repo.find_pending_of_batches.return_value = aiter([missing_record, sent_record])
    dyk_repo.find = AsyncMock(return_value=entry)
    dyk_repo.update_status = AsyncMock()

    user_repo = MagicMock()
    user_repo.find_users_by_ids.return_value = aiter(
        [user_doc_factory(user_id="u-sent", phone_number_id="911111111111")]
    )

    channel = MagicMock()
    channel.prepare_requests.return_value = [{"type": "template"}]
    channel.send_requests = AsyncMock(
        return_value=([channel_response_factory(status=200)], ["wamid-1"])
    )

    success, failure = await send_dyk.dispatch(
        dyk_repo,
        user_repo,
        channel,
        "batch-1",
        [LanguageCode.HINDI],
    )

    assert (success, failure) == (1, 0)
    assert dyk_repo.find.await_count == 1
    assert dyk_repo.update_status.await_args_list == [
        call(["record-missing"], "aborted"),
        call(["record-sent"], "completed"),
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("failure_mode", ["prepare", "response"])
async def test_dispatch_counts_failed_sends_without_completing_records(
    aiter,
    user_doc_factory,
    dyk_record_factory,
    dyk_entry_factory,
    channel_response_factory,
    failure_mode,
):
    record = dyk_record_factory(record_id="record-1", user_id="u-1")
    entry = dyk_entry_factory(entry_id=record.dyk_id)

    dyk_repo = MagicMock()
    dyk_repo.find_pending_of_batches.return_value = aiter([record])
    dyk_repo.find = AsyncMock(return_value=entry)
    dyk_repo.update_status = AsyncMock()

    user_repo = MagicMock()
    user_repo.find_users_by_ids.return_value = aiter(
        [user_doc_factory(user_id="u-1", phone_number_id="911111111111")]
    )

    channel = MagicMock()
    channel.send_requests = AsyncMock()

    if failure_mode == "prepare":
        channel.prepare_requests.return_value = []
    else:
        channel.prepare_requests.return_value = [{"type": "template"}]
        channel.send_requests.return_value = (
            [channel_response_factory(status=400, error="provider error")],
            ["wamid-1"],
        )

    success, failure = await send_dyk.dispatch(
        dyk_repo,
        user_repo,
        channel,
        "batch-1",
        [LanguageCode.HINDI],
    )

    assert (success, failure) == (0, 1)
    assert dyk_repo.update_status.await_args_list == [
        call([], "aborted"),
        call([], "completed"),
    ]

    if failure_mode == "prepare":
        channel.send_requests.assert_not_awaited()
    else:
        channel.send_requests.assert_awaited_once()


@pytest.mark.asyncio
async def test_main_retries_only_failed_batches(monkeypatch, user_factory, aiter):
    dyk_repo = MagicMock()
    dyk_repo.find_available_languages = AsyncMock(return_value=[LanguageCode.HINDI])
    dyk_repo.synchronize = AsyncMock(return_value=2)
    dyk_repo.find_pending_batch_ids.return_value = aiter(["batch-a", "batch-b"])

    user_repo = MagicMock()
    factory = MagicMock()
    factory.get_dyk_repository = AsyncMock(return_value=dyk_repo)
    factory.get_user_repository = AsyncMock(return_value=user_repo)

    async def fake_pick_candidates(*_args, **_kwargs):
        yield [user_factory(user_id="u-1", phone_number_id="911111111111")]
        yield [user_factory(user_id="u-2", phone_number_id="922222222222")]

    queue_mock = AsyncMock(side_effect=[("batch-a", 1, 0), ("batch-b", 1, 0)])
    dispatch_mock = AsyncMock(side_effect=[(1, 0), (0, 1), (1, 0)])
    sleep_mock = AsyncMock()

    monkeypatch.setattr(send_dyk, "get_repository_factory", AsyncMock(return_value=factory))
    monkeypatch.setattr(send_dyk, "pick_candidates", fake_pick_candidates)
    monkeypatch.setattr(send_dyk, "queue", queue_mock)
    monkeypatch.setattr(send_dyk, "dispatch", dispatch_mock)
    monkeypatch.setattr(send_dyk.asyncio, "sleep", sleep_mock)

    await send_dyk.main(["asha"], 2, MagicMock())

    assert queue_mock.await_count == 2
    assert [args.args[3] for args in dispatch_mock.await_args_list] == [
        "batch-a",
        "batch-b",
        "batch-b",
    ]
    sleep_mock.assert_awaited_once_with(2.5)


@pytest.mark.asyncio
async def test_main_stops_after_max_retries(monkeypatch, aiter):
    dyk_repo = MagicMock()
    dyk_repo.find_available_languages = AsyncMock(return_value=[LanguageCode.HINDI])
    dyk_repo.synchronize = AsyncMock(return_value=0)
    dyk_repo.find_pending_batch_ids.return_value = aiter(["batch-a"])

    user_repo = MagicMock()
    factory = MagicMock()
    factory.get_dyk_repository = AsyncMock(return_value=dyk_repo)
    factory.get_user_repository = AsyncMock(return_value=user_repo)

    async def fake_pick_candidates(*_args, **_kwargs):
        if False:
            yield []

    dispatch_mock = AsyncMock(side_effect=[(0, 1), (0, 1)])
    sleep_mock = AsyncMock()

    monkeypatch.setattr(send_dyk, "N_RETRIES", 2)
    monkeypatch.setattr(send_dyk, "get_repository_factory", AsyncMock(return_value=factory))
    monkeypatch.setattr(send_dyk, "pick_candidates", fake_pick_candidates)
    monkeypatch.setattr(send_dyk, "dispatch", dispatch_mock)
    monkeypatch.setattr(send_dyk.asyncio, "sleep", sleep_mock)

    await send_dyk.main(["asha"], 2, MagicMock())

    assert [args.args[3] for args in dispatch_mock.await_args_list] == [
        "batch-a",
        "batch-a",
    ]
    sleep_mock.assert_awaited_once_with(2.5)
