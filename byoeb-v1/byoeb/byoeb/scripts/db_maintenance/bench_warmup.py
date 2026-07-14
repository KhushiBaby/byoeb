"""
Warmup benchmark: run the same 11-day query 3x to confirm Motor connection reuse.
If 1st is slow and 2nd/3rd are fast → connection overhead only (prod will be fine).
If all 3 are slow → genuine per-query CosmosDB latency issue.

Run from byoeb-v1/byoeb/:
    python -m byoeb.scripts.db_maintenance.bench_warmup
"""
import asyncio
import os
import time
import certifi
from dotenv import load_dotenv
from pymongo.asynchronous.mongo_client import AsyncMongoClient
from pymongo.uri_parser import parse_uri

load_dotenv(os.path.join(os.path.dirname(__file__), "../../../keys.env"), override=True)

_PROJECTION = {
    "message_data.user.user_id": 1,
    "message_data.incoming_timestamp": 1,
    "message_data.outgoing_timestamp": 1,
    "message_data.message_category": 1,
    "message_data.message_context.additional_info": 1,
    "message_data.message_context.message_english_text": 1,
    "message_data.message_context.message_source_text": 1,
    "message_data.reply_context.reply_type": 1,
    "message_data.reply_context.reply_source_text": 1,
    "message_data.reply_context.reply_english_text": 1,
    "message_data.reply_context.additional_info": 1,
}

MESSAGE_CATEGORIES = [
    "audio_idk", "text_idk", "audio_disambiguation",
    "text_disambiguation", "bot_to_user_response",
]

# 11-day window
START_TS = 1746201600
END_TS   = 1747180799


async def main():
    connection_string = os.getenv("MONGO_DB_CONNECTION_STRING")
    if not connection_string:
        raise ValueError("MONGO_DB_CONNECTION_STRING not set")

    parsed = parse_uri(connection_string)
    db_name = parsed.get("database") or "ashadb"
    hosts = parsed.get("nodelist", [])
    is_localhost = any(h[0] in ("localhost", "127.0.0.1") for h in hosts)

    print(f"Connecting to {'localhost' if is_localhost else 'CosmosDB'} ...")
    t_connect = time.time()
    if is_localhost:
        client = AsyncMongoClient(connection_string, uuidRepresentation="standard", tz_aware=True, tls=False)
    else:
        client = AsyncMongoClient(connection_string, tlsCAFile=certifi.where(), uuidRepresentation="standard", tz_aware=True)

    col = client[db_name]["ashamessages"]
    query = {
        "message_data.incoming_timestamp": {"$gte": START_TS, "$lte": END_TS},
        "message_data.message_category": {"$in": MESSAGE_CATEGORIES},
    }

    for i in range(1, 4):
        t0 = time.time()
        docs = await col.find(query, _PROJECTION).to_list(5000)
        elapsed = time.time() - t0
        print(f"Run {i}: {len(docs)} docs in {elapsed:.2f}s")

    await client.close()


if __name__ == "__main__":
    asyncio.run(main())
