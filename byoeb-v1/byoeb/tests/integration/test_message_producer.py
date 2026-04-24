import time
import uuid


def _build_status_payload(*, bot_phone_number_id: str, username: str, phone_number_id: str, errors=None):
    username_slug = username.lower().replace(" ", "-")
    current_timestamp = str(int(time.time()))
    status = {
        "id": f"wamid.{username_slug}.{uuid.uuid4().hex}",
        "status": "sent",
        "timestamp": current_timestamp,
        "recipient_id": phone_number_id,
        "conversation": {
            "id": f"{username_slug}-conversation",
            "expiration_timestamp": current_timestamp,
            "origin": {"type": "service"},
        },
        "pricing": {
            "billable": False,
            "pricing_model": "PMP",
            "category": "service",
            "type": "free_customer_service",
        },
    }
    if errors is not None:
        status["errors"] = errors

    return {
        "object": "whatsapp_business_account",
        "entry": [
            {
                "id": "211506508713627",
                "changes": [
                    {
                        "value": {
                            "messaging_product": "whatsapp",
                            "statuses": [status],
                            "metadata": {
                                "display_phone_number": bot_phone_number_id,
                                "phone_number_id": bot_phone_number_id,
                            },
                        },
                        "field": "messages",
                    }
                ],
            }
        ],
    }


async def test_status_payload_without_errors_returns_success(envs, temp_user, whatsapp_webhook):
    with temp_user() as user:
        payload = _build_status_payload(bot_phone_number_id=envs.whatsapp_phone_number_id, username=user.user_name, phone_number_id=user.phone_number_id)
        response = whatsapp_webhook(payload)
    assert response.status_code == 200


def test_status_payload_with_errors_returns_success(envs, temp_user, whatsapp_webhook):
    with temp_user() as user:
        payload = _build_status_payload(bot_phone_number_id=envs.whatsapp_phone_number_id, username=user.user_name, phone_number_id=user.phone_number_id, errors=[{
            "code": 131000,
            "title": "Temporarily Unavailable",
            "message": "Upstream provider was not reachable",
        }])
        response = whatsapp_webhook(payload)
    assert response.status_code == 200
