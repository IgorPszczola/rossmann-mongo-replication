import os

from pymongo import MongoClient

# MongoDB backup connection
client_backup = MongoClient(os.getenv("MONGO_BACKUP_URI", "mongodb://rossmann_mongo_backup:27017/"))
db_backup = client_backup["rossmann_backup_db"]
backup_collection = db_backup["receipts_backup"]


def save_receipt(document):
    """Inserts or updates a receipt in the MongoDB backup database."""
    document_copy = document.copy()
    source_id = document_copy.pop("_id", None)

    if source_id is not None:
        document_copy["source_id"] = source_id

    transaction_id = document_copy["transaction_id"]
    backup_collection.update_one(
        {"transaction_id": transaction_id},
        {"$set": document_copy},
        upsert=True,
    )
    print(f"[MongoDB backup] Replicated receipt: {transaction_id}")


def delete_receipt(mongo_id=None, transaction_id=None):
    """Deletes a receipt from the MongoDB backup database."""
    if transaction_id:
        query = {"transaction_id": transaction_id}
    elif mongo_id:
        query = {"source_id": mongo_id}
    else:
        raise ValueError("MongoDB backup delete requires mongo_id or transaction_id")

    backup_collection.delete_one(query)
    print(f"[MongoDB backup] Deleted receipt matching: {transaction_id or mongo_id}")
