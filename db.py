import os
import time
from datetime import datetime
import psycopg2
import psycopg2.extras
from psycopg2.extras import Json
from bson import ObjectId

# PostgreSQL connection config from environment variables
PG_HOST = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT = os.getenv("POSTGRES_PORT", "5432")
PG_DB = os.getenv("POSTGRES_DB", "rossmann_relational_db")
PG_USER = os.getenv("POSTGRES_USER", "rossmann_user")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "rossmann_password")

# Register JSONB type globally for psycopg2
psycopg2.extras.register_default_jsonb(globally=True)

def get_pg_connection():
    """Attempts to connect to PostgreSQL in a loop (resilient to container startup lag)."""
    while True:
        try:
            print(f"Connecting to PostgreSQL ({PG_HOST}:{PG_PORT})...")
            conn = psycopg2.connect(
                host=PG_HOST,
                port=PG_PORT,
                database=PG_DB,
                user=PG_USER,
                password=PG_PASSWORD
            )
            print("Connected to PostgreSQL successfully!")
            return conn
        except psycopg2.OperationalError as e:
            print(f"PostgreSQL database is not ready yet ({e}). Retrying in 2 seconds...")
            time.sleep(2)

def clean_doc(val):
    """
    Recursively converts BSON-specific types (ObjectId, datetime) to standard JSON types.
    Prevents serialization errors when using psycopg2.extras.Json.
    """
    if isinstance(val, dict):
        return {k: clean_doc(v) for k, v in val.items()}
    elif isinstance(val, list):
        return [clean_doc(v) for v in val]
    elif isinstance(val, datetime):
        return val.isoformat()
    elif isinstance(val, ObjectId):
        return str(val)
    return val

def get_last_resume_token(conn):
    """Retrieves the last saved resume_token from the replication_state table."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT resume_token FROM replication_state WHERE pipeline_name = %s;",
                ("rossmann_replication",)
            )
            row = cur.fetchone()
            if row:
                return row[0]
    except Exception as e:
        print(f"Error reading resume_token: {e}")
    return None

def save_receipt_and_token(conn, transaction_id, timestamp, doc, resume_token):
    """Saves the receipt document and updates the resume token in a single ACID transaction."""
    clean_receipt_data = clean_doc(doc)
    clean_token = clean_doc(resume_token)

    try:
        with conn.cursor() as cur:
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
            
            # 2. Update the replication offset (resume_token)
            cur.execute(
                """
                INSERT INTO replication_state (pipeline_name, resume_token, updated_at)
                VALUES (%s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (pipeline_name) DO UPDATE
                SET resume_token = EXCLUDED.resume_token, updated_at = CURRENT_TIMESTAMP;
                """,
                ("rossmann_replication", Json(clean_token))
            )
            
        # Commit the transaction
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"SQL Transaction error, executed ROLLBACK: {e}")
        raise e
