import os

import pytest
import requests


HEADERS = {"accept": "application/json", "Content-Type": "application/json"}


def _env_or_skip():
    base_url = os.getenv("RECIEVE_URL")
    phone_number_id = os.getenv("PHONE_NUMBER_ID")
    user_name = os.getenv("USER_NAME", "Pytest User")
    if not base_url:
        pytest.skip("RECIEVE_URL not set")
    if not phone_number_id:
        pytest.skip("PHONE_NUMBER_ID not set")
    base = base_url.replace("receive", "")
    endpoints = {
        "register": f"{base}register_users",
        "update": f"{base}update_users",
        "delete": f"{base}delete_users",
        "get": f"{base}get_users",
    }
    return endpoints, phone_number_id, user_name


def test_register_user_endpoint():
    endpoints, phone_number_id, user_name = _env_or_skip()
    payload = [
        {
            "phone_number_id": phone_number_id,
            "user_location": {"district": "Pytest District"},
            "user_type": "asha",
            "user_language": "en",
            "user_name": user_name,
            "test_user": False,
        }
    ]
    with requests.Session() as session:
        session.delete(endpoints["delete"], headers=HEADERS, json=[phone_number_id])
        response = session.post(endpoints["register"], headers=HEADERS, json=payload)
        session.delete(endpoints["delete"], headers=HEADERS, json=[phone_number_id])
    response.raise_for_status()
    data = response.json()
    assert isinstance(data, list) and data
    assert data[0]["phone_number_id"] == phone_number_id


def test_get_users_endpoint():
    endpoints, phone_number_id, user_name = _env_or_skip()
    payload = [
        {
            "phone_number_id": phone_number_id,
            "user_location": {"district": "Pytest District"},
            "user_type": "asha",
            "user_language": "en",
            "user_name": user_name,
            "test_user": False,
        }
    ]
    with requests.Session() as session:
        session.delete(endpoints["delete"], headers=HEADERS, json=[phone_number_id])
        session.post(endpoints["register"], headers=HEADERS, json=payload).raise_for_status()
        response = session.post(endpoints["get"], headers={"Accept": "application/json"}, json=[phone_number_id])
        session.delete(endpoints["delete"], headers=HEADERS, json=[phone_number_id])
    response.raise_for_status()
    users = response.json()
    assert len(users) == 1
    assert users[0]["phone_number_id"] == phone_number_id
    assert users[0]["user_type"] == "asha"


def test_update_users_endpoint():
    endpoints, phone_number_id, user_name = _env_or_skip()
    register_payload = [
        {
            "phone_number_id": phone_number_id,
            "user_location": {"district": "Pytest District"},
            "user_type": "asha",
            "user_language": "en",
            "user_name": user_name,
            "test_user": False,
        }
    ]
    updated_name = f"{user_name} Updated"
    update_payload = [
        {
            "phone_number_id": phone_number_id,
            "user_name": updated_name,
            "user_type": "anm",
            "test_user": True,
        }
    ]
    with requests.Session() as session:
        session.delete(endpoints["delete"], headers=HEADERS, json=[phone_number_id])
        session.post(endpoints["register"], headers=HEADERS, json=register_payload).raise_for_status()
        response = session.post(endpoints["update"], headers=HEADERS, json=update_payload)
        response.raise_for_status()
        updated = session.post(endpoints["get"], headers={"Accept": "application/json"}, json=[phone_number_id])
        session.delete(endpoints["delete"], headers=HEADERS, json=[phone_number_id])
    updated.raise_for_status()
    user = updated.json()[0]
    assert user["user_name"] == updated_name
    assert user["user_type"] == "anm"
    assert user["test_user"] is True


def test_delete_users_endpoint():
    endpoints, phone_number_id, user_name = _env_or_skip()
    payload = [
        {
            "phone_number_id": phone_number_id,
            "user_location": {"district": "Pytest District"},
            "user_type": "asha",
            "user_language": "en",
            "user_name": user_name,
            "test_user": False,
        }
    ]
    with requests.Session() as session:
        session.delete(endpoints["delete"], headers=HEADERS, json=[phone_number_id])
        session.post(endpoints["register"], headers=HEADERS, json=payload).raise_for_status()
        response = session.delete(endpoints["delete"], headers=HEADERS, json=[phone_number_id])
    response.raise_for_status()
