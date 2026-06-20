from pymongo import MongoClient

# MongoDB backup connection
client_backup = MongoClient("mongodb://rossmann_mongo_backup:27017/")
db_backup = client_backup["rossmann_backup_db"]
backup_collection = db_backup["receipts_backup"]

def save_receipt(document):
    # Document copy
    document_copy = document.copy()

    if "_id" in document_copy:
        del document_copy["_id"]
        
    # Insert the clean document
    backup_collection.insert_one(document_copy)
    print(f"[MongoDB backup] Pomyślnie zreplikowano paragon dla klienta: {document_copy['customer']['first_name']}")