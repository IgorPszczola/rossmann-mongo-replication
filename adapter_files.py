import os
from datetime import datetime, timezone

from bson import json_util


class FileArchiveAdapter:
    """Appends MongoDB change-stream events to a local JSONL archive."""

    name = "File archive"

    def __init__(self, archive_path=None):
        self.archive_path = archive_path or os.getenv(
            "FILE_ARCHIVE_PATH",
            os.path.join("data", "replication_events.jsonl"),
        )
        self._seen_tokens = set()
        self._prepare_archive()
        self._load_seen_tokens()

    def handle(self, change):
        operation = change.get("operationType", "unknown")

        if operation == "insert":
            self.on_insert(change)
        elif operation == "update":
            self.on_update(change)
        elif operation == "delete":
            self.on_delete(change)
        else:
            self._append_event(operation, change)

    def on_insert(self, change):
        self._append_event("insert", change)

    def on_update(self, change):
        self._append_event("update", change)

    def on_delete(self, change):
        self._append_event("delete", change)

    def _prepare_archive(self):
        archive_dir = os.path.dirname(self.archive_path)
        if archive_dir:
            os.makedirs(archive_dir, exist_ok=True)

    def _load_seen_tokens(self):
        if not os.path.exists(self.archive_path):
            return

        with open(self.archive_path, "r", encoding="utf-8") as archive:
            for line in archive:
                try:
                    event = json_util.loads(line)
                except Exception:
                    continue

                token_key = event.get("resume_token_key")
                if token_key:
                    self._seen_tokens.add(token_key)

    def _append_event(self, operation, change):
        resume_token = change.get("_id")
        token_key = json_util.dumps(resume_token, sort_keys=True)

        if token_key in self._seen_tokens:
            print(f"[File archive] Event already archived, skipping duplicate: {operation}")
            return

        event = {
            "archived_at": datetime.now(timezone.utc).isoformat(),
            "operation": operation,
            "resume_token_key": token_key,
            "resume_token": resume_token,
            "namespace": change.get("ns"),
            "document_key": change.get("documentKey"),
            "full_document": change.get("fullDocument"),
            "update_description": change.get("updateDescription"),
        }

        with open(self.archive_path, "a", encoding="utf-8") as archive:
            archive.write(json_util.dumps(event, ensure_ascii=False) + "\n")

        self._seen_tokens.add(token_key)
        print(f"[File archive] Archived {operation} event to {self.archive_path}")
