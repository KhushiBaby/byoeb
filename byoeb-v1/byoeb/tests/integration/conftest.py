from contextlib import contextmanager
from byoeb.constants.user_enums import LanguageCode, UserType
from pydantic import AfterValidator
from typing import Annotated
from pydantic import AnyHttpUrl
from pydantic_settings import BaseSettings, SettingsConfigDict
from uuid import UUID
import hmac
import hashlib
import json

import pytest
import requests

from byoeb.services.auth.models import AuthUser

from byoeb_core.models.byoeb.user import User

class IntegrationTestEnvs(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="INTEGRATION_")

    base_url: Annotated[AnyHttpUrl, AfterValidator(lambda x: str(x).rstrip("/"))]
    auth_tenant_id: UUID
    auth_username: str
    auth_password: str

    # must match auth-tenant-id's configured whatsapp integration. used to generate
    # hmac signature for whatsapp webhooks when invoking POST /receive endpoint.
    whatsapp_app_secret: str
    whatsapp_phone_number_id: str


@pytest.fixture(scope="session")
def envs() -> IntegrationTestEnvs:
    return IntegrationTestEnvs()


@pytest.fixture(scope="session")
def whatsapp_webhook(envs):
    def _send(payload: dict):
        body = json.dumps(payload, separators=(",", ":"))
        signature = hmac.new(envs.whatsapp_app_secret.encode("utf-8"), body.encode("utf-8"), hashlib.sha256).hexdigest()
        headers = {"Content-Type": "application/json", "X-Hub-Signature-256": f"sha256={signature}"}
        response = requests.post(f"{envs.base_url}/receive", headers=headers, data=body, timeout=30)
        try:
            response.raise_for_status()
        except requests.HTTPError as e:
            raise RuntimeError(f"WhatsApp webhook POST /receive failed: {e}, response body: {response.text}") from e
        return response
    return _send


@pytest.fixture(scope="session")
def auth_access_token(auth_session):
    token = auth_session.cookies.get("asha_auth_token")
    if not token: raise RuntimeError("Auth cookie not set by /auth/token/issue.")
    return token


@pytest.fixture(scope="session")
def auth_session(envs):
    import time as _time
    session = requests.Session()
    headers = {"Content-Type": "application/x-www-form-urlencoded", "X-Tenant-ID": str(envs.auth_tenant_id)}
    for attempt in range(6):
        response = session.post(f"{envs.base_url}/auth/token/issue", headers=headers, data={"username": envs.auth_username, "password": envs.auth_password})
        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", 60))
            _time.sleep(retry_after)
            continue
        response.raise_for_status()
        break
    csrf_token = session.cookies.get("csrf_token")
    if not csrf_token: raise RuntimeError("CSRF cookie not set by /auth/token/issue.")
    session.headers.update({"X-CSRF-Token": csrf_token})
    yield session
    session.close()


@pytest.fixture
def auth_me(auth_session, envs) -> AuthUser:
    response = auth_session.get(f"{envs.base_url}/auth/me")
    response.raise_for_status()
    return AuthUser(**response.json())


@pytest.fixture
def temp_user(envs, auth_session, auth_me):
    @contextmanager
    def _create_temp_user(user_type: UserType = UserType.ASHA, test_user: bool = False, lang: LanguageCode = LanguageCode.ENGLISH, **kwargs):
        payload = {
            "phone_number_id": auth_me.phone_number_id,
            "user_location": {"district": "Pytest District"},
            "user_type": user_type.value,
            "user_language": lang.value,
            "user_name": envs.auth_username,
            "test_user": test_user,
            "tenant_id": str(envs.auth_tenant_id),
            **kwargs
        }

        auth_session.delete(f"{envs.base_url}/delete_users", json=[payload["phone_number_id"]]).raise_for_status()
        auth_session.post(f"{envs.base_url}/register_users", json=[payload]).raise_for_status()
        try:
            response = auth_session.post(f"{envs.base_url}/get_users", json=[payload["phone_number_id"]])
            response.raise_for_status()
            yield User.model_validate(response.json()[0])
        finally:
            auth_session.delete(f"{envs.base_url}/delete_users", json=[payload["phone_number_id"]])

    return _create_temp_user