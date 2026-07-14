"""
One-time script to create the compound index on ashamessages collection.

Run from byoeb-v1/byoeb/ directory:
    python -m byoeb.scripts.db_maintenance.create_message_index

To check progress without blocking (run in a separate terminal while index builds):
    python -m byoeb.scripts.db_maintenance.create_message_index --check
"""
import argparse
import asyncio
import os
import certifi
from dotenv import load_dotenv
from pymongo import ASCENDING
from pymongo.asynchronous.mongo_client import AsyncMongoClient
from pymongo.uri_parser import parse_uri

load_dotenv(os.path.join(os.path.dirname(__file__), "../../../keys.env"), override=True)

TARGET_INDEX = "message_data.incoming_timestamp_1_message_data.message_category_1"


async def get_client_and_collection():
    connection_string = os.getenv("MONGO_DB_CONNECTION_STRING")
    if not connection_string:
        raise ValueError("MONGO_DB_CONNECTION_STRING not set in keys.env")

    parsed = parse_uri(connection_string)
    db_name = parsed.get("database") or "ashadb"
    hosts = parsed.get("nodelist", [])
    is_localhost = any(h[0] in ("localhost", "127.0.0.1") for h in hosts)

    if is_localhost:
        client = AsyncMongoClient(connection_string, uuidRepresentation="standard", tz_aware=True, tls=False)
    else:
        client = AsyncMongoClient(connection_string, tlsCAFile=certifi.where(), uuidRepresentation="standard", tz_aware=True)

    collection = client[db_name]["ashamessages"]
    return client, collection, db_name


async def check_progress():
    client, collection, db_name = await get_client_and_collection()
    try:
        indexes = await collection.index_information()
        if TARGET_INDEX in indexes:
            print(f"[DONE] Index '{TARGET_INDEX}' exists on {db_name}.ashamessages")
        else:
            print(f"[PENDING] Index not yet present on {db_name}.ashamessages")
            print("Existing indexes:", list(indexes.keys()))
    finally:
        await client.close()


async def create_index():
    client, collection, db_name = await get_client_and_collection()
    try:
        # Check if already exists
        indexes = await collection.index_information()
        if TARGET_INDEX in indexes:
            print(f"[SKIP] Index already exists: {TARGET_INDEX}")
            return

        doc_count = await collection.estimated_document_count()
        print(f"Collection has ~{doc_count:,} documents. Starting index build ...")
        print("This blocks until CosmosDB confirms the index — may take several minutes.")

        index_name = await collection.create_index(
            [("message_data.incoming_timestamp", ASCENDING), ("message_data.message_category", ASCENDING)],
            background=True,
        )
        print(f"[DONE] Index created: {index_name}")

        indexes = await collection.index_information()
        print("All indexes:", list(indexes.keys()))
    finally:
        await client.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true", help="Check if index exists (non-blocking)")
    args = parser.parse_args()

    if args.check:
        asyncio.run(check_progress())
    else:
        asyncio.run(create_index())
