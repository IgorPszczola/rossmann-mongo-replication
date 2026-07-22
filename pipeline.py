import os
import time

from pymongo import MongoClient

from adapter_files import FileArchiveAdapter
from adapter_mongodb import save_receipt as save_mongo_backup
from postgres_target import PostgresTarget
from adapter_redis import save_receipt as save_redis_cache

MONGO_URI = os.getenv("MONGO_URI", "mongodb://rossmann_mongo:27017/")
RETRY_DELAY_SECONDS = int(os.getenv("RETRY_DELAY_SECONDS", "5"))
MAX_RETRIES = int(os.getenv("REPLICATION_MAX_RETRIES", "0"))


class MongoBackupAdapter:
    name = "MongoDB backup"

    def handle(self, change):
        operation = change.get("operationType")
        if operation != "insert":
            print(f"[MongoDB backup] Skipping unsupported operation: {operation}")
            return

        save_mongo_backup(change["fullDocument"])


class PostgresReceiptAdapter:
    name = "PostgreSQL receipts"

    def __init__(self, target):
        self.target = target

    def handle(self, change):
        operation = change.get("operationType")
        if operation != "insert":
            print(f"[PostgreSQL] Skipping unsupported operation: {operation}")
            return

        doc = change["fullDocument"]
        self.target.save(
            doc["transaction_id"],
            doc["timestamp"],
            doc,
        )

    def reconnect(self):
        self.target.close()
        self.target.connect(keep_retry=False)


class RedisCacheAdapter:
    name = "Redis cache"

    def handle(self, change):
        operation = change.get("operationType")
        if operation != "insert":
            print(f"[Redis Cache] Skipping unsupported operation: {operation}")
            return

        save_redis_cache(change["fullDocument"])


def _retry_limit_reached(attempt):
    return MAX_RETRIES > 0 and attempt >= MAX_RETRIES


def _reconnect_adapter(adapter):
    reconnect = getattr(adapter, "reconnect", None)
    if not callable(reconnect):
        return

    try:
        reconnect()
    except Exception as reconnect_error:
        print(f"[Retry] Reconnect failed for {adapter.name}: {reconnect_error}")


def broadcast_with_retry(change, adapters):
    attempt = 1

    while True:
        failed_adapter = None

        try:
            for adapter in adapters:
                failed_adapter = adapter
                adapter.handle(change)
            return
        except Exception as error:
            adapter_name = failed_adapter.name if failed_adapter else "unknown adapter"
            print(f"[Error] {adapter_name} failed on attempt {attempt}: {error}")
            print("[Error] Resume token was not advanced. Retrying keeps targets in sync.")

            _reconnect_adapter(failed_adapter)

            if _retry_limit_reached(attempt):
                raise

            print(f"[Retry] Retrying full broadcast in {RETRY_DELAY_SECONDS} seconds...")
            time.sleep(RETRY_DELAY_SECONDS)
            attempt += 1


def save_offset_with_retry(pg_target, resume_token):
    attempt = 1

    while True:
        try:
            pg_target.save_resume_token(resume_token)
            return
        except Exception as error:
            print(f"[Error] Failed to save resume token on attempt {attempt}: {error}")
            print("[Error] All targets are written, but stream offset is not confirmed yet.")

            try:
                pg_target.close()
                pg_target.connect(keep_retry=False)
            except Exception as reconnect_error:
                print(f"[Retry] PostgreSQL reconnect failed: {reconnect_error}")

            if _retry_limit_reached(attempt):
                raise

            print(f"[Retry] Retrying offset save in {RETRY_DELAY_SECONDS} seconds...")
            time.sleep(RETRY_DELAY_SECONDS)
            attempt += 1


def print_change_summary(change):
    operation = change.get("operationType")

    if operation == "insert":
        doc = change["fullDocument"]
        transaction_id = doc["transaction_id"]
        customer_name = doc["customer"]["first_name"]
        amount = doc["payment"]["amount_paid"]

        print(f"NEW RECEIPT CAPTURED! Operation: {operation}")
        print(f"Customer: {customer_name}, Amount: {amount} PLN (ID: {transaction_id})")
        return

    print(f"CHANGE CAPTURED! Operation: {operation}, Document: {change.get('documentKey')}")


def open_change_stream(collection, resume_token):
    watch_options = {"full_document": "updateLookup"}

    if not resume_token:
        return collection.watch(**watch_options)

    try:
        return collection.watch(resume_after=resume_token, **watch_options)
    except Exception as error:
        print(f"\n[Warning] Failed to resume from saved token: {error}")
        print("Starting change stream from now...")
        return collection.watch(**watch_options)


def main():
    pg_target = PostgresTarget()
    pg_target.connect()

    adapters = [
        MongoBackupAdapter(),
        FileArchiveAdapter(),
        PostgresReceiptAdapter(pg_target),
        RedisCacheAdapter(),
    ]

    client = MongoClient(MONGO_URI)
    db = client["rossmann_db"]
    receipts_collection = db["receipts"]

    resume_token = pg_target.get_last_resume_token()

    print("\nStarting Change Stream listener on 'receipts' collection...")
    if resume_token:
        print("Resuming stream from the saved resume token in PostgreSQL.")
    else:
        print("No saved resume token found. Listening from now.")

    stream = open_change_stream(receipts_collection, resume_token)

    print("Waiting for new receipts. Press Ctrl+C to terminate.\n")

    try:
        with stream:
            for change in stream:
                print_change_summary(change)
                broadcast_with_retry(change, adapters)
                save_offset_with_retry(pg_target, change["_id"])
                print("SAVED to all adapters + replication offset updated.")
                print("-" * 50)

    except KeyboardInterrupt:
        print("\nListener terminated by user.")
    finally:
        pg_target.close()
        client.close()


if __name__ == "__main__":
    main()
