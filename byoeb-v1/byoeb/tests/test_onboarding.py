import pytest
import httpx
import uuid
import time

# Endpoint
BASE_URL = "http://127.0.0.1:5000/receive"


USER_WA_ID = "917567071072"  # Phone number to onboard
BOT_WA_ID = "183958451475612"

# Payload templates for the 4-step onboarding flow
ONBOARDING_PAYLOADS = [
   {
    "object": "whatsapp_business_account",
    "entry": [
        {
            "id": "211506508713627",
            "changes": [
                {
                    "value": {
                        "messaging_product": "whatsapp",
                        "metadata": {
                            "display_phone_number": "919001386867",
                            "phone_number_id": "183958451475612"
                        },
                        "contacts": [
                            {
                                "profile": {
                                    "name": "Nikunj"
                                },
                                "wa_id": "919929959548"
                            }
                        ],
                        "messages": [
                            {
                                "from": "917567071072",
                                "id": "wamid.HBgMOTE3NTY3MDcxMDcyFQIAEhgUM0EzNzYyNTE4REMxMjA2RjM4QABZA",
                                "timestamp": "1758573528",
                                "text": {
                                    "body": "hi"
                                },
                                "type": "text"
                            }
                        ]
                    },
                    "field": "messages"
                }
            ]
        }
    ]
}
,
   {
  "object": "whatsapp_business_account",
  "entry": [
    {
      "id": "211506508713627",
      "changes": [
        {
                        "value": {
            "messaging_product": "whatsapp",
            "metadata": {
              "display_phone_number": "919001386867",
              "phone_number_id": "183958451475612"
            },
            "contacts": [
              {
                "profile": { "name": "Nikunj" },
                "wa_id": "917567071072"
              }
            ],
            "messages": [
              {
                "from": "917567071072",
                "id": "wamid.USER_INTERACTIVE_REPLY_ID_ABCDEFGHIJKLMNOPQRSTUVWX",
                "timestamp": "1758576570",
                "context": {
                  "from": "183958451475612",
                  "id": "wamid.HBgMOTE3NTY3MDcxMDcyFQIAERgSMzI2NUE2M0M4QzE4NzQwMTdGAA=="
                },
                "type": "interactive",
                "interactive": {
                  "type": "list_reply",
                  "list_reply": {
                    "id": "English",
                    "title": "English",
                    "description": ""
                  }
                }
              }
            ]
          },
          "field": "messages"
        }
      ]
    }
  ]
}
,{
  "object": "whatsapp_business_account",
  "entry": [
    {
      "id": "211506508713627",
"changes": [
        {
          "value": {
            "messaging_product": "whatsapp",
            "metadata": {
              "display_phone_number": "919001386867",
              "phone_number_id": "183958451475612"
            },
            "contacts": [
              {
                "profile": { "name": "Nikunj" },
                "wa_id": "917567071072"
              }
            ],
            "messages": [
              {
                "from": "917567071072",
                "id": "wamid.USER_BUTTON_REPLY_ID_ABCDEFGHIJKLMNOPQRSTUVW",
                "timestamp": "1758576682",
                "context": {
                  "from": "183958451475612",
                  "id": "wamid.HBgMOTE3NTY3MDcxMDcyFQIAERgSOTc5NUQ5MjI3MkJFQzIzODlBAA=="
                },
                "type": "interactive",
                "interactive": {
                  "type": "button_reply",
                  "button_reply": {
                    "id": "others",
                    "title": "Others"
                  }
                }
              }
            ]
          },
          "field": "messages"
        }
      ]
    }
  ]
}
,
    {
  "object": "whatsapp_business_account",
  "entry": [
    {
      "id": "211506508713627",
      "changes": [
        {
          "value": {
            "messaging_product": "whatsapp",
            "metadata": {
              "display_phone_number": "919001386867",


              "phone_number_id": "183958451475612"
            },
            "contacts": [
              {
                "profile": { "name": "Nikunj" },
                "wa_id": "917567071072"
              }
            ],
            "messages": [
              {
                "from": "917567071072",
                "id": "wamid.USER_CONSENT_REPLY_ID_ABCDEFGHIJKLMNOPQRSTUVW",
                "timestamp": "1758577083",
                "context": {
                  "from": "183958451475612",
                  "id": "wamid.HBgMOTE3NTY3MDcxMDcyFQIAERgSOTQ5MkY3QjVGRUM3OEU0OUMwAA=="
                },
                "type": "interactive",
                "interactive": {
                  "type": "button_reply",
                  "button_reply": {
                    "id": "yes",
                    "title": "Yes"
                  }
                }
              }
            ]
          },
          "field": "messages"
        }
      ]
    }
  ]
}

]

def get_current_timestamp():
    return str(int(time.time()))

def generate_message_id():
    return f"wamid.{uuid.uuid4().hex}"

@pytest.mark.asyncio
async def test_whatsapp_onboarding_flow():
    previous_bot_wamid = generate_message_id()

    async with httpx.AsyncClient() as client:
        for step, payload_template in enumerate(ONBOARDING_PAYLOADS):
            payload = payload_template.copy()
            msg = payload["entry"][0]["changes"][0]["value"]["messages"][0]
            msg["id"] = generate_message_id()
            msg["timestamp"] = get_current_timestamp()
            if "context" not in msg or msg.get("type") == "interactive":
            	msg["context"] = {}
            msg["context"]["id"] = previous_bot_wamid
            response = await client.post(BASE_URL, json=payload)
            assert response.status_code == 200, f"Step {step+1} failed with status {response.status_code}"

            print(f"Step {step+1} completed. Response: {response.text}")

        # Generate a new simulated bot wamid for the next step
            previous_bot_wamid = generate_message_id()



         

