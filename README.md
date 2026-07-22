# Rossmann Mongo Replication

## Dead Letter Queue

The listener retries a failed replication event three times by default. After the
last failed attempt, it appends the complete MongoDB change, adapter name and error
details to `data/dead_letter_queue.jsonl`, advances the resume token and continues
with the next event.

Configuration:

- `REPLICATION_MAX_RETRIES` - maximum number of replication attempts (`3` by default,
  `0` means unlimited retries).
- `RETRY_DELAY_SECONDS` - delay between attempts in seconds (`5` by default).
- `DEAD_LETTER_QUEUE_PATH` - path to the JSONL dead letter queue file.
- `OFFSET_MAX_RETRIES` - limit for saving the resume token (`0` keeps retrying until
  the offset is safely stored).
