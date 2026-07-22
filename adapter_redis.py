import os
import json
from datetime import datetime
import redis

# Redis connection
redis_host = os.getenv("REDIS_HOST", "rossmann_redis")
redis_port = int(os.getenv("REDIS_PORT", 6379))
client_redis = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)

def save_receipt(document):
    # Document copy
    document_copy = document.copy()

    if "_id" in document_copy:
        del document_copy["_id"]

    # Helper to serialize datetime to ISO format strings
    def json_serializer(obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        raise TypeError(f"Type {type(obj)} not serializable")

    transaction_id = document_copy["transaction_id"]
    key = f"receipt:{transaction_id}"

    # Store serialized JSON string
    client_redis.set(key, json.dumps(document_copy, default=json_serializer))
    print(f"[Redis Cache] Successfully replicated receipt for customer: {document_copy['customer']['first_name']}")
