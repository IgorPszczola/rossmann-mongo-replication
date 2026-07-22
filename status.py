import os
import json
import sys
from datetime import datetime
from pymongo import MongoClient
import redis

try:
    from postgres_target import PostgresTarget
except ImportError:
    PostgresTarget = None

# Helper to normalize documents for exact comparison
def normalize(data):
    if isinstance(data, dict):
        return {k: normalize(v) for k, v in data.items() if k != "_id"}
    elif isinstance(data, list):
        return [normalize(item) for item in data]
    elif isinstance(data, datetime):
        return data.isoformat()
    return data

def get_mongo_client(uris):
    for uri in uris:
        try:
            client = MongoClient(uri, serverSelectionTimeoutMS=1000)
            client.server_info()
            return client
        except Exception:
            continue
    return None

def get_redis_client(connections):
    for host, port in connections:
        try:
            client = redis.Redis(host=host, port=port, decode_responses=True, socket_timeout=1)
            client.ping()
            return client
        except Exception:
            continue
    return None

def get_postgres_data():
    if not PostgresTarget:
        print("[WARNING] PostgresTarget class is not available.")
        return None
    try:
        target = PostgresTarget()
        # Set a short timeout/non-blocking connect to avoid hanging
        target.connect(keep_retry=False)
        with target.conn.cursor() as cur:
            cur.execute("SELECT transaction_id, data FROM receipts;")
            rows = cur.fetchall()
        target.close()
        return {row[0]: row[1] for row in rows}
    except Exception as e:
        print(f"[ERROR] Could not connect to PostgreSQL: {e}")
        return None

def main():
    # Setup connection attempts
    mongo_source_uris = [
        os.getenv("MONGO_SOURCE_URI", ""),
        "mongodb://rossmann_mongo:27017/",
        "mongodb://localhost:27017/"
    ]
    mongo_source_uris = [u for u in mongo_source_uris if u]

    mongo_backup_uris = [
        os.getenv("MONGO_BACKUP_URI", ""),
        "mongodb://rossmann_mongo_backup:27017/",
        "mongodb://localhost:27018/"
    ]
    mongo_backup_uris = [u for u in mongo_backup_uris if u]

    redis_connections = []
    if os.getenv("REDIS_HOST"):
        redis_connections.append((os.getenv("REDIS_HOST"), int(os.getenv("REDIS_PORT", 6379))))
    redis_connections.extend([("rossmann_redis", 6379), ("localhost", 6379)])

    print("Connecting to databases...")
    
    client_source = get_mongo_client(mongo_source_uris)
    client_backup = get_mongo_client(mongo_backup_uris)
    client_redis = get_redis_client(redis_connections)
    pg_data = get_postgres_data()

    # Status check for core connections
    connections_ok = True
    if not client_source:
        print("[ERROR] Could not connect to MongoDB Source!")
        connections_ok = False
    if not client_backup:
        print("[ERROR] Could not connect to MongoDB Backup!")
        connections_ok = False
    if not client_redis:
        print("[ERROR] Could not connect to Redis Cache!")
        connections_ok = False

    if not connections_ok:
        print("\nStatus check aborted due to missing connections.")
        sys.exit(1)

    db_source = client_source["rossmann_db"]
    receipts_source = db_source["receipts"]

    db_backup = client_backup["rossmann_backup_db"]
    receipts_backup = db_backup["receipts_backup"]

    source_count = receipts_source.count_documents({})
    backup_count = receipts_backup.count_documents({})
    
    redis_keys = client_redis.keys("receipt:*")
    redis_count = len(redis_keys)

    print("=" * 60)
    print("                 DATABASE STATUS REPORT                     ")
    print("=" * 60)
    print(f"MongoDB Source (rossmann_db):       {source_count} records")
    print(f"MongoDB Backup (rossmann_backup):   {backup_count} records")
    print(f"Redis Cache (rossmann_redis):       {redis_count} records")
    if pg_data is not None:
        print(f"PostgreSQL (rossmann_relational):   {len(pg_data)} records")
    else:
        print(f"PostgreSQL (rossmann_relational):   CONNECTION ERROR/NOT ACTIVE")
    print("-" * 60)

    # Consistency checks
    inconsistencies = []
    source_docs = list(receipts_source.find({}))
    source_tx_ids = {doc.get("transaction_id") for doc in source_docs}

    for src_doc in source_docs:
        tx_id = src_doc.get("transaction_id")
        norm_src = normalize(src_doc)
        
        # Check in Backup MongoDB
        back_doc = receipts_backup.find_one({"transaction_id": tx_id})
        if not back_doc:
            inconsistencies.append({
                "transaction_id": tx_id,
                "db": "MongoDB Backup",
                "error": "Missing record in backup database"
            })
        else:
            norm_back = normalize(back_doc)
            if norm_src != norm_back:
                inconsistencies.append({
                    "transaction_id": tx_id,
                    "db": "MongoDB Backup",
                    "error": "Data content mismatch"
                })

        # Check in Redis
        redis_val = client_redis.get(f"receipt:{tx_id}")
        if not redis_val:
            inconsistencies.append({
                "transaction_id": tx_id,
                "db": "Redis Cache",
                "error": "Missing record in Redis cache"
            })
        else:
            try:
                redis_doc = json.loads(redis_val)
                norm_redis = normalize(redis_doc)
                if norm_src != norm_redis:
                    inconsistencies.append({
                        "transaction_id": tx_id,
                        "db": "Redis Cache",
                        "error": "Data content mismatch"
                    })
            except Exception as e:
                inconsistencies.append({
                    "transaction_id": tx_id,
                    "db": "Redis Cache",
                    "error": f"JSON parsing error in Redis: {str(e)}"
                })

        # Check in PostgreSQL
        if pg_data is not None:
            if tx_id not in pg_data:
                inconsistencies.append({
                    "transaction_id": tx_id,
                    "db": "PostgreSQL",
                    "error": "Missing record in PostgreSQL database"
                })
            else:
                norm_pg = normalize(pg_data[tx_id])
                if norm_src != norm_pg:
                    inconsistencies.append({
                        "transaction_id": tx_id,
                        "db": "PostgreSQL",
                        "error": "Data content mismatch"
                    })

    # Check in Backup
    backup_docs = list(receipts_backup.find({}))
    for back_doc in backup_docs:
        tx_id = back_doc.get("transaction_id")
        if tx_id not in source_tx_ids:
            inconsistencies.append({
                "transaction_id": tx_id,
                "db": "MongoDB Backup",
                "error": "Extra record (missing in Source)"
            })

    # Check in Redis
    for key in redis_keys:
        tx_id = key.split(":", 1)[1] if ":" in key else key
        if tx_id not in source_tx_ids:
            inconsistencies.append({
                "transaction_id": tx_id,
                "db": "Redis Cache",
                "error": "Extra record (missing in Source)"
            })

    # Check in PostgreSQL
    if pg_data is not None:
        for tx_id in pg_data.keys():
            if tx_id not in source_tx_ids:
                inconsistencies.append({
                    "transaction_id": tx_id,
                    "db": "PostgreSQL",
                    "error": "Extra record (missing in Source)"
                })

    # Display results
    if not inconsistencies:
        print(" Consistency status: CONSISTENT (No anomalies detected)")
    else:
        print(f" Consistency status: INCONSISTENT ({len(inconsistencies)} anomalies detected!)")
        print("-" * 60)
        for inc in inconsistencies:
            print(f"- [ID: {inc['transaction_id']}] Database: {inc['db']} | Issue: {inc['error']}")

    print("=" * 60)

if __name__ == "__main__":
    main()
