# db_merchant.py (FINAL – Merchant Only)
import os
import uuid
import psycopg2
import psycopg2.extras
from contextlib import contextmanager

DATABASE_URL = os.environ.get("DATABASE_URL")

# -------------------------
# Connection helper
# -------------------------

@contextmanager
def get_conn():
    conn = psycopg2.connect(DATABASE_URL)
    try:
        yield conn
    finally:
        conn.close()


# -------------------------
# MERCHANT HELPERS
# -------------------------

def get_or_create_merchant_by_phone(phone: str) -> dict:
    with get_conn() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            "SELECT * FROM merchants WHERE phone = %s",
            (phone,)
        )
        merchant = cur.fetchone()

        if merchant:
            return merchant

        cur.execute(
            """
            INSERT INTO merchants (phone)
            VALUES (%s)
            RETURNING *
            """,
            (phone,)
        )
        conn.commit()
        return cur.fetchone()


def get_merchant_by_id(merchant_id: int) -> dict:
    with get_conn() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            "SELECT * FROM merchants WHERE id = %s",
            (merchant_id,)
        )
        return cur.fetchone()


# -------------------------
# TRANSCRIPTION JOB QUEUE
# -------------------------

def create_transcription_job(merchant_id: int, gcs_path: str) -> str:
    job_id = str(uuid.uuid4())

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO transcription_jobs (id, merchant_id, gcs_path, status)
            VALUES (%s, %s, %s, 'PENDING')
            """,
            (job_id, merchant_id, gcs_path)
        )
        conn.commit()

    return job_id


def fetch_next_pending_job() -> dict | None:
    """
    Fetch ONE pending job and lock it.
    Safe for multiple workers.
    """
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, merchant_id, gcs_path
                FROM transcription_jobs
                WHERE status = 'PENDING'
                ORDER BY created_at
                LIMIT 1
                FOR UPDATE SKIP LOCKED
                """
            )
            job = cur.fetchone()

            if not job:
                return None

            # Immediately mark PROCESSING
            cur.execute(
                """
                UPDATE transcription_jobs
                SET status = 'PROCESSING', updated_at = NOW()
                WHERE id = %s
                """,
                (job["id"],)
            )
            conn.commit()

            return job


def mark_job_done(job_id: str):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            UPDATE transcription_jobs
            SET status = 'DONE', updated_at = NOW()
            WHERE id = %s
            """,
            (job_id,)
        )
        conn.commit()


def mark_job_failed(job_id: str, error: str):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            UPDATE transcription_jobs
            SET status = 'FAILED', error = %s, updated_at = NOW()
            WHERE id = %s
            """,
            (error, job_id)
        )
        conn.commit()


# -------------------------
# MERCHANT MEMORY (LEDGER)
# -------------------------

def save_merchant_memory(
    merchant_id: int,
    content: str,
    source: str = "voice",
    contact_id: int | None = None
):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO merchant_memory
                (merchant_id, contact_id, source, content)
            VALUES (%s, %s, %s, %s)
            """,
            (merchant_id, contact_id, source, content)
        )
        conn.commit()
