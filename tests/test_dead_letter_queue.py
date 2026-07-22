import os
import tempfile
import unittest
from unittest.mock import patch

from bson import json_util

import pipeline
from dead_letter_queue import DeadLetterQueue


class SelectiveAdapter:
    name = "test adapter"

    def __init__(self, rejected_transaction_id):
        self.rejected_transaction_id = rejected_transaction_id
        self.calls = []

    def handle(self, change):
        transaction_id = change["fullDocument"]["transaction_id"]
        self.calls.append(transaction_id)
        if transaction_id == self.rejected_transaction_id:
            raise ValueError("invalid receipt")


def make_change(transaction_id, token):
    return {
        "_id": {"token": token},
        "operationType": "insert",
        "ns": {"db": "rossmann_db", "coll": "receipts"},
        "documentKey": {"_id": transaction_id},
        "fullDocument": {"transaction_id": transaction_id},
    }


class DeadLetterQueueTest(unittest.TestCase):
    def test_malformed_receipt_summary_does_not_stop_processing(self):
        change = make_change("broken", "token-1")
        change["fullDocument"]["customer"] = "invalid"
        change["fullDocument"]["payment"] = []

        pipeline.print_change_summary(change)

    def test_failed_change_is_queued_and_next_change_is_processed(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            queue_path = os.path.join(temp_dir, "dead_letter_queue.jsonl")
            queue = DeadLetterQueue(queue_path)
            adapter = SelectiveAdapter("broken")

            with patch.object(pipeline, "MAX_RETRIES", 2), patch.object(
                pipeline, "RETRY_DELAY_SECONDS", 0
            ):
                first_result = pipeline.broadcast_with_retry(
                    make_change("broken", "token-1"), [adapter], queue
                )
                second_result = pipeline.broadcast_with_retry(
                    make_change("valid", "token-2"), [adapter], queue
                )

            self.assertFalse(first_result)
            self.assertTrue(second_result)
            self.assertEqual(adapter.calls, ["broken", "broken", "valid"])

            with open(queue_path, "r", encoding="utf-8") as queue_file:
                entries = [json_util.loads(line) for line in queue_file]

            self.assertEqual(len(entries), 1)
            self.assertEqual(entries[0]["transaction_id"], "broken")
            self.assertEqual(entries[0]["failed_adapter"], "test adapter")
            self.assertEqual(entries[0]["attempts"], 2)
            self.assertEqual(entries[0]["error_type"], "ValueError")

    def test_same_resume_token_is_not_written_twice(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            queue_path = os.path.join(temp_dir, "dead_letter_queue.jsonl")
            queue = DeadLetterQueue(queue_path)
            change = make_change("broken", "same-token")

            queue.save(change, "test adapter", ValueError("first"), 3)
            reloaded_queue = DeadLetterQueue(queue_path)
            reloaded_queue.save(change, "test adapter", ValueError("second"), 3)

            with open(queue_path, "r", encoding="utf-8") as queue_file:
                self.assertEqual(len(queue_file.readlines()), 1)


if __name__ == "__main__":
    unittest.main()
