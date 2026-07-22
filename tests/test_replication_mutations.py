import unittest
from unittest.mock import Mock, call, patch

from bson import ObjectId

import adapter_mongodb
import adapter_redis
import pipeline


def make_document(source_id=None):
    return {
        "_id": source_id or ObjectId(),
        "transaction_id": "tx-123",
        "timestamp": "2026-07-22T20:00:00Z",
        "customer": {"first_name": "Test"},
        "payment": {"amount_paid": 12.34},
    }


class BackupAdapterTest(unittest.TestCase):
    def test_save_preserves_source_id_and_delete_uses_it(self):
        source_id = ObjectId()
        collection = Mock()

        with patch.object(adapter_mongodb, "backup_collection", collection):
            adapter_mongodb.save_receipt(make_document(source_id))
            adapter_mongodb.delete_receipt(mongo_id=source_id)

        update_filter, update = collection.update_one.call_args.args
        self.assertEqual(update_filter, {"transaction_id": "tx-123"})
        self.assertEqual(update["$set"]["source_id"], source_id)
        self.assertNotIn("_id", update["$set"])
        collection.delete_one.assert_called_once_with({"source_id": source_id})


class RedisAdapterTest(unittest.TestCase):
    def test_save_and_delete_maintain_source_id_lookup(self):
        source_id = ObjectId()
        client = Mock()
        client.get.return_value = "tx-123"

        with patch.object(adapter_redis, "client_redis", client):
            adapter_redis.save_receipt(make_document(source_id))
            adapter_redis.delete_receipt(source_id=source_id)

        source_key = f"receipt-source:{source_id}"
        client.set.assert_any_call(source_key, "tx-123")
        self.assertIn(call("receipt:tx-123"), client.delete.call_args_list)
        self.assertIn(call(source_key), client.delete.call_args_list)


class PipelineMutationRoutingTest(unittest.TestCase):
    def test_update_is_sent_to_all_mutable_targets(self):
        document = make_document()
        change = {"operationType": "update", "fullDocument": document}
        pg_target = Mock()

        with patch.object(pipeline, "save_mongo_backup") as save_mongo, patch.object(
            pipeline, "save_redis_cache"
        ) as save_redis:
            pipeline.MongoBackupAdapter().handle(change)
            pipeline.PostgresReceiptAdapter(pg_target).handle(change)
            pipeline.RedisCacheAdapter().handle(change)

        save_mongo.assert_called_once_with(document)
        pg_target.update.assert_called_once_with(
            document["transaction_id"], document["timestamp"], document
        )
        save_redis.assert_called_once_with(document)

    def test_delete_is_sent_to_all_mutable_targets(self):
        source_id = ObjectId()
        change = {"operationType": "delete", "documentKey": {"_id": source_id}}
        pg_target = Mock()

        with patch.object(pipeline, "delete_mongo_backup") as delete_mongo, patch.object(
            pipeline, "delete_redis_cache"
        ) as delete_redis:
            pipeline.MongoBackupAdapter().handle(change)
            pipeline.PostgresReceiptAdapter(pg_target).handle(change)
            pipeline.RedisCacheAdapter().handle(change)

        delete_mongo.assert_called_once_with(mongo_id=source_id)
        pg_target.delete.assert_called_once_with(source_id)
        delete_redis.assert_called_once_with(source_id=source_id)


if __name__ == "__main__":
    unittest.main()
