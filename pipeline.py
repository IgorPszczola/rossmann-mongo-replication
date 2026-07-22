import os
import time

from pymongo import MongoClient

from adapter_files import FileArchiveAdapter
from adapter_mongodb import (
    delete_receipt as delete_mongo_backup,
    save_receipt as save_mongo_backup,
)
from adapter_redis import save_receipt as save_redis_cache
from dead_letter_queue import DeadLetterQueue
from postgres_target import PostgresTarget


MONGO_URI = os.getenv("MONGO_URI", "mongodb://rossmann_mongo:27017/")
RETRY_DELAY_SECONDS = int(os.getenv("RETRY_DELAY_SECONDS", "5"))
MAX_RETRIES = int(os.getenv("REPLICATION_MAX_RETRIES", "3"))
OFFSET_MAX_RETRIES = int(os.getenv("OFFSET_MAX_RETRIES", "0"))


class MongoBackupAdapter:
    name = "MongoDB backup"

    def handle(self, change):
        operation = change.get("operationType")

        if operation in ["insert", "update"]:
            doc = change.get("fullDocument")
            if doc:
                save_mongo_backup(doc)
            else:
                print(
                    f"[MongoDB backup] Warning: No fullDocument present "
                    f"for operation: {operation}"
                )
        elif operation == "delete":
            document_key = change.get("documentKey", {})
            mongo_id = document_key.get("_id")
            if mongo_id:
                delete_mongo_backup(mongo_id=mongo_id)
            else:
                print("[MongoDB backup] Warning: Cannot delete without documentKey _id")
        else:
            print(f"[MongoDB backup] Skipping unsupported operation: {operation}")


class PostgresReceiptAdapter:
    name = "PostgreSQL receipts"

    def __init__(self, target):
        self.target = target

    def handle(self, change):
        operation = change.get("operationType")

        if operation == "insert":
            doc = change["fullDocument"]
            self.target.save(
                doc["transaction_id"],
                doc["timestamp"],
                doc,
            )
        elif operation == "update":
            doc = change.get("fullDocument")
            if doc and "transaction_id" in doc:
                self.target.update(
                    doc["transaction_id"],
                    doc["timestamp"],
                    doc,
                )
            else:
                print(
                    "[PostgreSQL] Warning: Cannot update without "
                    "fullDocument/transaction_id"
                )
        elif operation == "delete":
            document_key = change.get("documentKey", {})
            mongo_id = document_key.get("_id")
            if mongo_id:
                self.target.delete(mongo_id)
            else:
                print("[PostgreSQL] Warning: Cannot delete without documentKey _id")
        else:
            print(f"[PostgreSQL] Skipping unsupported operation: {operation}")

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
