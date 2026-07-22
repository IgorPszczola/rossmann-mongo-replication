import os
import time
from datetime import datetime
import psycopg2
import psycopg2.extras
from psycopg2.extras import Json
from bson import ObjectId

class PostgresTarget:
    def __init__(self):
        self.host = os.getenv("POSTGRES_HOST", "localhost")
        self.port = os.getenv("POSTGRES_PORT", "5432")
        self.db_name = os.getenv("POSTGRES_DB", "rossmann_relational_db")
        self.user = os.getenv("POSTGRES_USER", "rossmann_user")
        self.password = os.getenv("POSTGRES_PASSWORD", "rossmann_password")
        self.conn = None

        # Register JSONB type globally for psycopg2
        psycopg2.extras.register_default_jsonb(globally=True)

    def connect(self, keep_retry=True):
        """Connects to PostgreSQL with a retry loop for container lag."""
        while True:
            try:
                print(f"Connecting to PostgreSQL ({self.host}:{self.port})...")
                self.conn = psycopg2.connect(
                    host=self.host,
                    port=self.port,
                    database=self.db_name,
                    user=self.user,
                    password=self.password
                )
                print("Connected to PostgreSQL successfully!")
                break
            except psycopg2.OperationalError as e:
                if not keep_retry:
                    raise
                print(f"PostgreSQL is not ready yet ({e}). Retrying in 2 seconds...")
                time.sleep(2)

    def _clean_doc(self, val):
        """Recursively converts BSON-specific types to standard JSON types."""
        if isinstance(val, dict):
            return {k: self._clean_doc(v) for k, v in val.items()}
        elif isinstance(val, list):
            return [self._clean_doc(v) for v in val]
        elif isinstance(val, datetime):
            return val.isoformat()
        elif isinstance(val, ObjectId):
            return str(val)
        return val

    def get_last_resume_token(self):
        """Retrieves the last saved resume_token for this target."""
        if not self.conn:
            return None
        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    "SELECT resume_token FROM replication_state WHERE pipeline_name = %s;",
                    ("rossmann_replication",)
                )
                row = cur.fetchone()
                if row:
                    return row[0]
        except Exception as e:
            print(f"Error reading resume_token from PostgreSQL: {e}")
        return None

    def _write_resume_token(self, cur, clean_token):
        cur.execute(
            """
            INSERT INTO replication_state (pipeline_name, resume_token, updated_at)
            VALUES (%s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT (pipeline_name) DO UPDATE
            SET resume_token = EXCLUDED.resume_token, updated_at = CURRENT_TIMESTAMP;
            """,
            ("rossmann_replication", Json(clean_token))
        )

    def save_resume_token(self, resume_token):
        """Saves the latest processed change-stream resume token."""
        if not self.conn:
            raise ConnectionError("PostgreSQL connection is not established.")

        clean_token = self._clean_doc(resume_token)

        try:
            with self.conn.cursor() as cur:
                self._write_resume_token(cur, clean_token)

            self.conn.commit()
        except Exception as e:
            try:
                self.conn.rollback()
            except Exception:
                pass
            print(f"PostgreSQL offset error, executed ROLLBACK: {e}")
            raise e

    def save(self, transaction_id, timestamp, doc, resume_token=None):
        """Saves or inserts receipt data."""
        if not self.conn:
            raise ConnectionError("PostgreSQL connection is not established.")
            
        clean_receipt_data = self._clean_doc(doc)
        clean_token = self._clean_doc(resume_token) if resume_token is not None else None

        try:
            with self.conn.cursor() as cur:
                # 1. Insert/upsert the receipt
                cur.execute(
                    """
                    INSERT INTO receipts (transaction_id, timestamp, data)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (transaction_id) DO UPDATE 
                    SET timestamp = EXCLUDED.timestamp, data = EXCLUDED.data;
                    """,
                    (transaction_id, timestamp, Json(clean_receipt_data))
                )
                
                if clean_token is not None:
                    self._write_resume_token(cur, clean_token)
                
            self.conn.commit()
        except Exception as e:
            try:
                self.conn.rollback()
            except Exception:
                pass
            print(f"PostgreSQL Transaction error, executed ROLLBACK: {e}")
            raise e

    def update(self, transaction_id, timestamp, doc):
        """Updates an existing receipt in PostgreSQL."""
        if not self.conn:
            raise ConnectionError("PostgreSQL connection is not established.")

        clean_receipt_data = self._clean_doc(doc)

        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE receipts 
                    SET timestamp = %s, data = %s, replicated_at = CURRENT_TIMESTAMP
                    WHERE transaction_id = %s;
                    """,
                    (timestamp, Json(clean_receipt_data), transaction_id)
                )
            self.conn.commit()
            print(f"[PostgreSQL] Updated receipt: {transaction_id}")
        except Exception as e:
            try:
                self.conn.rollback()
            except Exception:
                pass
            print(f"PostgreSQL Update error, executed ROLLBACK: {e}")
            raise e

    def delete(self, identifier):
        """Deletes a receipt from PostgreSQL by its transaction_id or MongoDB _id."""
        if not self.conn:
            raise ConnectionError("PostgreSQL connection is not established.")

        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    """
                    DELETE FROM receipts 
                    WHERE transaction_id = %s OR data->>'_id' = %s;
                    """,
                    (str(identifier), str(identifier))
                )
            self.conn.commit()
            print(f"[PostgreSQL] Deleted receipt matching identifier: {identifier}")
        except Exception as e:
            try:
                self.conn.rollback()
            except Exception:
                pass
            print(f"PostgreSQL Delete error, executed ROLLBACK: {e}")
            raise e

    def close(self):
        """Closes the connection safely."""
        if self.conn:
            self.conn.close()
            self.conn = None
            print("PostgreSQL connection closed.")
