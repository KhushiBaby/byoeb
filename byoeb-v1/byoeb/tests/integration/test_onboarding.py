import pytest
import httpx
import uuid
import time
import requests
import json
import re
from urllib.parse import quote
import os
# Endpoint
BASE_URL = os.getenv("RECIEVE_URL")
PHONE_NUMBER_ID = os.getenv("PHONE_NUMBER_ID")
USER_NAME = os.getenv("USER_NAME")
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
                                    "name": USER_NAME
                                },
                                "wa_id": PHONE_NUMBER_ID
                            }
                        ],
                        "messages": [
                            {
                                "from": PHONE_NUMBER_ID,
                                "id": "",
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
                "profile": {
                                    "name": USER_NAME
                                },
                                "wa_id": PHONE_NUMBER_ID
              }
            ],
            "messages": [
              {
                "from": PHONE_NUMBER_ID,
                "id": "",
                "timestamp": "1758576570",
                "context": {
                  "from": "183958451475612",
                  "id": ""
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
                "profile": { "name": USER_NAME },
                "wa_id": PHONE_NUMBER_ID
              }
            ],
            "messages": [
              {
                "from": PHONE_NUMBER_ID,
                "id": "",
                "timestamp": "1758576682",
                "context": {
                  "from": "183958451475612",
                  "id": ""
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
                "profile": { "name": USER_NAME },
                "wa_id": PHONE_NUMBER_ID
              }
            ],
            "messages": [
              {
                "from": PHONE_NUMBER_ID,
                "id": "",
                "timestamp": "1758577083",
                "context": {
                  "from": "183958451475612",
                  "id": ""
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
},
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
                "profile": { "name": USER_NAME },
                "wa_id": PHONE_NUMBER_ID
              }
            ],
            "messages": [
              {
                "from": PHONE_NUMBER_ID,
                                "id": "",
                                "timestamp": "",
                                "text": {
                                    "body": "What is a antra injection?"
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
]

def get_current_timestamp():
    return str(int(time.time()))

def generate_message_id():
    return f"wamid.{uuid.uuid4().hex}"

def test_whatsapp_onboarding_flow():
    context_id = None
    print("Starting onboarding flow test...")
    x=0
    m_id=[]
    delete_url = BASE_URL.replace("receive","delete_users")
    headers = {
    "accept": "application/json",
    "Content-Type": "application/json"
    }
    data = [PHONE_NUMBER_ID] 

    response = requests.delete(delete_url, headers=headers, data=json.dumps(data))
    response.raise_for_status()
    print("Delete user response:", response.status_code, response.text)

    c_id=[]
    for step, payload_template in enumerate(ONBOARDING_PAYLOADS):
            payload = payload_template.copy()
            msg = payload["entry"][0]["changes"][0]["value"]["messages"][0]
            msg["id"] = generate_message_id()
            msg["timestamp"] = get_current_timestamp()
            m_id.append(msg["id"])
            if step > 0 and step!=4:
                msg["context"]["id"] = context_id
                c_id.append(context_id)
                print("context_id:",context_id)
            print("-------------------------STEP",step+1,"-------------------------")
            response = requests.post(BASE_URL, json=payload)
            url = BASE_URL.replace("receive","get_bot_messages?timestamp=")+str(msg["timestamp"])
            print("paylod: ",payload)
            bot_message = requests.get(url)
            bot_message=bot_message.json()

            valid_timestamps = [ int(str(m["outgoing_timestamp"])) for m in bot_message if m.get("outgoing_timestamp") not in (None, "None", "")]
            max_timestamp=max(valid_timestamps) if valid_timestamps else 0
            print("max_timestamp:",max_timestamp, "msg timestamp:", msg['timestamp'])
          
                  
            if step<len(ONBOARDING_PAYLOADS)-2:
              print("Waiting for bot response...")

              while max_timestamp<int(msg['timestamp']):
                  time.sleep(5)
                  print("Checking for new bot messages...")
                  bot_message = requests.get(url)
                  bot_message=bot_message.json()
                  valid_timestamps1 = [ int(str(m["outgoing_timestamp"])) for m in bot_message if m.get("outgoing_timestamp") not in (None, "None", "")]
                  max_timestamp=max(valid_timestamps1) if valid_timestamps1 else 0
                  print("max_timestamp in while:",max_timestamp)
              print("Bot response received.")
              for i in bot_message:
                  if i["reply_context"]["reply_id"]==msg["id"] and i["outgoing_timestamp"]!=None and  int(str(i["outgoing_timestamp"]))>int(msg['timestamp']):
                    if "language" in i["message_context"]["message_source_text"] and step==0:   
                        context_id=i["message_context"]["message_id"]
                        #print(i)
                        break 
                    elif "Who are you" in i["message_context"]["message_source_text"] and step==1:
                        context_id=i["message_context"]["message_id"]
                        #print(i)
                        break
                    elif "Researchers" in i["message_context"]["message_source_text"] and step==2:
                        context_id=i["message_context"]["message_id"]
                        #print(i)
                        break
                    elif step==4 and "pregnancy" in i["message_context"]["message_source_text"]:
                        context_id=i["message_context"]["message_id"]
                        #print(i)
                        break
                
              print(f"Step {step+1} response: {response.json()}")
              assert response.status_code == 200, f"Step {step+1} failed"

    get_url = BASE_URL.replace("receive","get_users")
    response = requests.post(get_url, headers={"Accept": "application/json"}, json=[PHONE_NUMBER_ID])
    response.raise_for_status()
    message = response.json()
    assert len(message) == 1


