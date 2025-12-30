# worker_merchant.py

import time
import os
import traceback
from google.cloud import storage

from db_merchant import (
    fetch_next_pending_job,
    mark_job_done,
    mark_job_failed,
    save_merchant_memory,
    get_merchant_by_id,
)
from utils import (
    send_whatsapp,
    transcribe_file_multilang,
)

POLL_INTERVAL_SECONDS = 5
TMP_DIR = "/tmp"


def download_audio_from_gcs(gcs_path: str) -> str:
    """
    Download audio file from GCS to local temp directory.
    Returns local file path.
    """
    client = storage.Client()

    # gs://bucket-name/path/to/file.oga
    path = gcs_path.replace("gs://", "")
    bucket_name, object_name = path.split("/", 1)

    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_name)

    local_filename = os.path.join(TMP_DIR, os.path.basename(object_name))
    blob.download_to_filename(local_filename)

    return local_filename


def worker_loop():
    print("🟢 MinA Merchant Worker started")

    while True:
        job = fetch_next_pending_job()

        if not job:
            time.sleep(POLL_INTERVAL_SECONDS)
            continue

        job_id = job["id"]
        merchant_id = job["merchant_id"]
        gcs_path = job["gcs_path"]

        try:
            # 1️⃣ Download audio
            local_audio_path = download_audio_from_gcs(gcs_path)

            # 2️⃣ Transcribe
            transcript = transcribe_file_multilang(local_audio_path)

            if not transcript or not transcript.strip():
                raise Exception("Empty transcription")

            # 3️⃣ Save merchant memory (ledger)
            save_merchant_memory(
                merchant_id=merchant_id,
                content=transcript,
                source="voice",
            )

            # 4️⃣ Mark job done
            mark_job_done(job_id)

            # 5️⃣ Notify merchant
            # We fetch phone via merchant table
            # (safe: phone is unique identifier)
            merchant = get_merchant_by_id(merchant_id)

            send_whatsapp(
                merchant["phone"],
                f"📝 Yaad rakh liya:\n\n{transcript}"
            )

        except Exception as e:
            print("❌ Worker error:", traceback.format_exc())
            mark_job_failed(job_id, str(e))

            

        finally:
            # Cleanup local temp file
            try:
                if "local_audio_path" in locals():
                    os.remove(local_audio_path)
            except Exception:
                pass


if __name__ == "__main__":
    worker_loop()
