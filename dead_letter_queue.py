import os
from datetime import datetime, timezone

from bson import json_util


class DeadLetterQueue:
    """Stores changes that could not be replicated after all attempts."""

    def __init__(self, queue_path=None):
        self.queue_path = queue_path or os.getenv(
            "DEAD_LETTER_QUEUE_PATH",
            os.path.join("data", "dead_letter_queue.jsonl"),
        )
        self._seen_tokens = set()
        self._prepare_queue()
        self._load_seen_tokens()

    def save(self, change, failed_adapter, error, attempts):
        resume_token = change.get("_id")
        token_key = json_util.dumps(resume_token, sort_keys=True)

        if resume_token is not None and token_key in self._seen_tokens:
            print("[DLQ] Change already exists in the dead letter queue, skipping duplicate.")
            return

        full_document = change.get("fullDocument") or {}
        transaction_id = (
            full_document.get("transaction_id")
            if isinstance(full_document, dict)
            else None
        )
        event = {
            "dead_lettered_at": datetime.now(timezone.utc).isoformat(),
            "failed_adapter": failed_adapter,
            "attempts": attempts,
            "error_type": type(error).__name__,
            "error_message": str(error),
            "resume_token_key": token_key,
            "resume_token": resume_token,
            "operation": change.get("operationType"),
            "namespace": change.get("ns"),
            "document_key": change.get("documentKey"),
            "transaction_id": transaction_id,
            "change": change,
        }

        with open(self.queue_path, "a", encoding="utf-8") as queue:
            queue.write(json_util.dumps(event, ensure_ascii=False) + "\n")
            queue.flush()
            os.fsync(queue.fileno())

        if resume_token is not None:
            self._seen_tokens.add(token_key)

        print(f"[DLQ] Change saved to {self.queue_path}")

    def _prepare_queue(self):
        queue_dir = os.path.dirname(self.queue_path)
        if queue_dir:
            os.makedirs(queue_dir, exist_ok=True)

    def _load_seen_tokens(self):
        if not os.path.exists(self.queue_path):
            return

        with open(self.queue_path, "r", encoding="utf-8") as queue:
            for line in queue:
                try:
                    event = json_util.loads(line)
                except Exception:
                    continue

                token_key = event.get("resume_token_key")
                if token_key:
                    self._seen_tokens.add(token_key)
