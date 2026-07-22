import json
import os
from datetime import datetime

import redis

# Redis connection
redis_host = os.getenv("REDIS_HOST", "rossmann_redis")
redis_port = int(os.getenv("REDIS_PORT", 6379))
client_redis = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)


def _json_serializer(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")


def save_receipt(document):
    """Inserts or updates a receipt and its source-id lookup in Redis."""
    document_copy = document.copy()
    source_id = document_copy.pop("_id", None)
    transaction_id = document_copy["transaction_id"]

    client_redis.set(
        f"receipt:{transaction_id}",
        json.dumps(document_copy, default=_json_serializer),
    )

    if source_id is not None:
        client_redis.set(f"receipt-source:{source_id}", transaction_id)

    print(f"[Redis Cache] Replicated receipt: {transaction_id}")


def delete_receipt(source_id=None, transaction_id=None):
    """Deletes a cached receipt using its transaction or source MongoDB id."""
    source_key = f"receipt-source:{source_id}" if source_id is not None else None

    if transaction_id is None and source_key:
        transaction_id = client_redis.get(source_key)

    if transaction_id is not None:
        client_redis.delete(f"receipt:{transaction_id}")
    if source_key:
        client_redis.delete(source_key)

    print(f"[Redis Cache] Deleted receipt matching: {transaction_id or source_id}")
