import os

from pymongo import MongoClient

# MongoDB backup connection
client_backup = MongoClient(os.getenv("MONGO_BACKUP_URI", "mongodb://rossmann_mongo_backup:27017/"))
db_backup = client_backup["rossmann_backup_db"]
backup_collection = db_backup["receipts_backup"]

def save_receipt(document):
    # Document copy
    document_copy = document.copy()

    if "_id" in document_copy:
        del document_copy["_id"]
        
    # Upsert keeps retries idempotent if a later adapter fails.
    backup_collection.update_one(
        {"transaction_id": document_copy["transaction_id"]},
        {"$set": document_copy},
        upsert=True,
    )
    print(f"[MongoDB backup] Pomyślnie zreplikowano paragon dla klienta: {document_copy['customer']['first_name']}")
