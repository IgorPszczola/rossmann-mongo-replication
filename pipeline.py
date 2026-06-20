import os
from pymongo import MongoClient
from db import get_pg_connection, get_last_resume_token, save_receipt_and_token

# MongoDB connection URI config
MONGO_URI = os.getenv("MONGO_URI", "mongodb://rossmann_mongo:27017/")

def main():
    # Initialize connection to PostgreSQL
    pg_conn = get_pg_connection()
    
    client = MongoClient(MONGO_URI)
    db = client["rossmann_db"]
    receipts_collection = db["receipts"]

    # Retrieve the last saved change stream offset from PostgreSQL
    resume_token = get_last_resume_token(pg_conn)
    
    print("\nStarting Change Stream listener on 'receipts' collection...")
    if resume_token:
        print("Resuming stream from the saved resume token in PostgreSQL.")
    else:
        print("No saved resume token found. Listening from now.")

    # Initialize the MongoDB change stream
    stream = None
    if resume_token:
        try:
            stream = receipts_collection.watch(resume_after=resume_token)
        except Exception as e:
            print(f"\n[Warning] Failed to resume from saved token (it might have expired in MongoDB): {e}")
            print("Starting change stream from now...")
            stream = receipts_collection.watch()
    else:
        stream = receipts_collection.watch()

    print("Waiting for new receipts. Press Ctrl+C to terminate.\n")

    try:
        with stream:
            for change in stream:
                operation = change["operationType"]
                
                if operation == "insert":
                    doc = change["fullDocument"]
                    token = change["_id"]
                    
                    transaction_id = doc["transaction_id"]
                    timestamp = doc["timestamp"]
                    customer_name = doc["customer"]["first_name"]
                    amount = doc["payment"]["amount_paid"]
                    
                    print(f"NEW RECEIPT CAPTURED! Operation: {operation}")
                    print(f"Customer: {customer_name}, Amount: {amount} PLN (ID: {transaction_id})")
                    
                    # Save to PostgreSQL and update the replication offset
                    save_receipt_and_token(pg_conn, transaction_id, timestamp, doc, token)
                    print("SAVED to PostgreSQL + replication offset updated.")
                    print("-" * 50)
                    
    except KeyboardInterrupt:
        print("\nListener terminated by user.")
    finally:
        pg_conn.close()
        client.close()

if __name__ == "__main__":
    main()