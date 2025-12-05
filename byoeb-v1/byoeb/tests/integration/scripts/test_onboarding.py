import os
import sys

from openpyxl import Workbook, load_workbook
import pytest
import requests

from byoeb.scripts import onboarding


BASE_URL = os.getenv("RECIEVE_URL", "http://0.0.0.0:8000").replace("receive", "").rstrip("/")
DUMMY_PHONE_NUMBERS = ["9990000000001", "9990000000002"]


@pytest.fixture
def onboarding_excel(tmp_path) -> str:
    workbook = Workbook()
    sheet = workbook.active
    sheet.append([
        "user_id",
        "user_name",
        "phone_number_id",
        "user_location",
        "user_type",
        "test_user",
        "created_timestamp",
        "user_language",
    ])
    sheet.append([
        "user-1",
        "First User",
        int(DUMMY_PHONE_NUMBERS[0]),
        "{'district': 'Test District 1'}",
        "asha",
        True,
        1_700_000_000,
        "en",
    ])
    sheet.append([
        "user-2",
        "Second User",
        int(DUMMY_PHONE_NUMBERS[1]),
        "{'district': 'Test District 2'}",
        "anm",
        False,
        1_700_000_100,
        "hi",
    ])

    file_path = tmp_path / "onboarding.xlsx"
    workbook.save(file_path)
    return str(file_path)


@pytest.fixture
def cleanup_dummy_users():
    requests.delete(f"{BASE_URL}/delete_users", headers={"Content-Type": "application/json", "Accept": "application/json"}, json=DUMMY_PHONE_NUMBERS)
    yield DUMMY_PHONE_NUMBERS
    requests.delete(f"{BASE_URL}/delete_users", headers={"Content-Type": "application/json", "Accept": "application/json"}, json=DUMMY_PHONE_NUMBERS)


def test_onboarding_registers_and_exports_users(monkeypatch, onboarding_excel, cleanup_dummy_users, tmp_path):
    output_sheet = tmp_path / "output.xlsx"
    monkeypatch.setattr(sys, "argv", ["onboarding.py", "--file", onboarding_excel, "--url", BASE_URL, "--sheet", str(output_sheet)])
    onboarding.main()

    get_response = requests.post(f"{BASE_URL}/get_users", headers={"Accept": "application/json", "Content-Type": "application/json"}, json=cleanup_dummy_users)
    get_response.raise_for_status()
    users = get_response.json()

    assert len(users) == len(cleanup_dummy_users)
    returned_numbers = {str(user.get("phone_number_id")) for user in users}
    assert returned_numbers == set(cleanup_dummy_users)
    assert all(
        isinstance(user.get("user_location"), dict) and user["user_location"].get("district")
        for user in users
    )

    assert output_sheet.exists()
    sheet = load_workbook(output_sheet).active
    rows = list(sheet.iter_rows(values_only=True))
    header, data_rows = rows[0], rows[1:]
    exported_rows = [dict(zip(header, row)) for row in data_rows]
    exported_numbers = {str(row.get("phone")) for row in exported_rows}
    assert exported_numbers == set(cleanup_dummy_users)
    assert all("district" in str(row.get("location", "")).lower() for row in exported_rows)


def test_onboarding_updates_users(monkeypatch, onboarding_excel, cleanup_dummy_users):
    monkeypatch.setattr(sys, "argv", ["onboarding.py", "--file", onboarding_excel, "--url", BASE_URL, "--update"])
    onboarding.main()

    get_response = requests.post(f"{BASE_URL}/get_users", headers={"Accept": "application/json", "Content-Type": "application/json"}, json=cleanup_dummy_users)
    get_response.raise_for_status()
    users = get_response.json()
    assert len(users) == len(cleanup_dummy_users)
