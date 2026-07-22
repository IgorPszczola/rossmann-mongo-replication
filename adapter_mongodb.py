import os
from pymongo import MongoClient

# MongoDB backup connection
client_backup = MongoClient(os.getenv("MONGO_BACKUP_URI", "mongodb://rossmann_mongo_backup:27017/"))
db_backup = client_backup["rossmann_backup_db"]
backup_collection = db_backup["receipts_backup"]

def save_receipt(document):
    """Inserts or updates a receipt in the MongoDB backup database."""
    document_copy = document.copy()

    if "_id" in document_copy:
        del document_copy["_id"]
        
    # Upsert keeps retries idempotent for inserts and updates
    backup_collection.update_one(
        {"transaction_id": document_copy["transaction_id"]},
        {"$set": document_copy},
        upsert=True,
    )
    print(f"[MongoDB backup] Successfully replicated receipt for customer: {document_copy['customer']['first_name']}")

def delete_receipt(mongo_id=None, transaction_id=None):
    """Deletes a receipt from the MongoDB backup database."""
    if transaction_id:
        backup_collection.delete_one({"transaction_id": transaction_id})
    elif mongo_id:
        backup_collection.delete_one({"_id": mongo_id})
    print(f"[MongoDB backup] Deleted receipt from backup matching: {transaction_id or mongo_id}")
