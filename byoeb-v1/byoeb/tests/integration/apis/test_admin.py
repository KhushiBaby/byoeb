import csv
import io
from datetime import datetime, timedelta, timezone

import pytest


def test_post_asha_logs_streams_csv_with_attachment_header(envs, auth_session):
    now = datetime.now(timezone.utc)
    start = now - timedelta(minutes=30)
    response = auth_session.post(f"{envs.base_url}/asha_logs", data={"start": start.isoformat(), "end": now.isoformat()})
    response.raise_for_status()
    assert response.headers.get("content-type", "").startswith("text/csv")
    content_disposition = response.headers.get("content-disposition", "")
    assert "attachment;" in content_disposition
    assert "asha-logs" in content_disposition


def test_post_asha_logs_requires_end_datetime(envs, auth_session):
    now = datetime.now(timezone.utc)
    response = auth_session.post(f"{envs.base_url}/asha_logs", data={"start": (now - timedelta(minutes=15)).isoformat()})
    assert response.status_code == 422
    detail = response.json().get("detail", [])
    assert any(item.get("loc", [None])[-1] == "end" for item in detail)


def test_post_asha_logs_requires_start_datetime(envs, auth_session):
    now = datetime.now(timezone.utc)
    response = auth_session.post(f"{envs.base_url}/asha_logs", data={"end": now.isoformat()})
    assert response.status_code == 422
    detail = response.json().get("detail", [])
    assert any(item.get("loc", [None])[-1] == "start" for item in detail)


def test_post_asha_logs_rejects_invalid_datetime_format(envs, auth_session):
    now = datetime.now(timezone.utc)
    response = auth_session.post(f"{envs.base_url}/asha_logs", data={"start": "not-a-date", "end": now.isoformat()})
    assert response.status_code == 422
    detail = response.json().get("detail", [])
    assert any(item.get("loc", [None])[-1] == "start" for item in detail)


def test_post_asha_logs_stream_is_parsable_and_sorted(envs, auth_session):
    bot_messages_url = f"{envs.base_url}/get_bot_messages"
    bot_response = auth_session.get(bot_messages_url, params={"timestamp": 0, "length": 100})
    bot_response.raise_for_status()
    bot_messages = bot_response.json()
    if not bot_messages:
        pytest.skip("No bot messages available to test asha logs streaming response")

    incoming_timestamps = []
    for message in bot_messages:
        incoming_ts = message.get("incoming_timestamp")
        if incoming_ts in (None, "", "None"):
            continue
        try:
            incoming_timestamps.append(int(incoming_ts))
        except (TypeError, ValueError):
            continue

    if not incoming_timestamps:
        pytest.skip("Bot messages do not contain usable incoming timestamps")

    start = datetime.fromtimestamp(min(incoming_timestamps), tz=timezone.utc)
    end = min(
        datetime.fromtimestamp(max(incoming_timestamps), tz=timezone.utc),
        datetime.now(timezone.utc) - timedelta(seconds=1)
    )

    response = auth_session.post(f"{envs.base_url}/asha_logs", data={"start": start.isoformat(), "end": end.isoformat()})
    response.raise_for_status()

    reader = csv.DictReader(io.StringIO(response.content.decode()))
    rows = list(reader)

    if not rows:
        pytest.skip("Asha logs streamed response returned no rows")

    first_row = rows[0]
    for field in ("phone_number_id", "query_source", "answer_source"):
        assert field in first_row and first_row[field] != ""

    incoming_timestamp_values = [int(row["incoming_timestamp"]) for row in rows if row.get("incoming_timestamp")]
    assert incoming_timestamp_values == sorted(incoming_timestamp_values, reverse=True)
