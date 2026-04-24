from byoeb.constants.user_enums import LanguageCode, UserType


def test_get_users_endpoint(auth_me, temp_user):
    with temp_user(user_type=UserType.ASHA, lang=LanguageCode.ENGLISH) as user:
        assert user.phone_number_id == auth_me.phone_number_id
        assert user.user_type == UserType.ASHA.value


def test_update_users_endpoint(envs, auth_session, temp_user):
    with temp_user(user_type=UserType.ASHA, lang=LanguageCode.ENGLISH, test_user=False) as user:
        auth_session.post(f"{envs.base_url}/update_users", json=[{
            "phone_number_id": user.phone_number_id,
            "user_name": f"{envs.auth_username} Updated",
            "user_type": UserType.ANM.value,
            "test_user": True,
        }]).raise_for_status()

        updated = auth_session.post(f"{envs.base_url}/get_users", json=[user.phone_number_id])
        updated.raise_for_status()
        user = updated.json()[0]

    assert user["user_name"] == f"{envs.auth_username} Updated"
    assert user["user_type"] == UserType.ANM.value
    assert user["test_user"] is True


def test_delete_users_endpoint(envs, auth_session, temp_user):
    with temp_user() as user:
        phone_number_id = user.phone_number_id

    response = auth_session.post(f"{envs.base_url}/get_users", json=[phone_number_id])
    response.raise_for_status()
    assert response.json() == []  # user should be deleted after exiting the context manager