import requests
import logging
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from minio import Minio
from minio.error import S3Error
from io import BytesIO

start_date = "20241110_100000"
end_date = "20250308_070000"

leaderboard_link = (
    "https://www.vendeeglobe.org/sites/default/files/ranking/"
    "vendeeglobe_leaderboard_YYYYMMDD_HHMMSS.xlsx"
)

MINIO_ENDPOINT = "nginx:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"
MINIO_BUCKET = "raw"

minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

if not minio_client.bucket_exists(MINIO_BUCKET):
    minio_client.make_bucket(MINIO_BUCKET)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


def generate_timestamps(start_date: str, end_date: str, step_hours: int = 4):
    start = datetime.strptime(start_date, "%Y%m%d_%H%M%S")
    end = datetime.strptime(end_date, "%Y%m%d_%H%M%S")
    current = start
    while current <= end:
        yield current.strftime("%Y%m%d_%H%M%S")
        current += timedelta(hours=step_hours)


def file_exists_in_minio(key: str) -> bool:
    try:
        minio_client.stat_object(MINIO_BUCKET, key)
        return True
    except S3Error:
        return False


def download_file(ts: str):
    object_name = f"leaderboard_{ts}.xlsx"

    if file_exists_in_minio(object_name):
        return f"⏩ {ts} skipped (already exists in MinIO)"

    url = leaderboard_link.replace("YYYYMMDD_HHMMSS", ts)

    try:
        r = requests.get(url, timeout=10)
        if r.status_code == 200 and len(r.content) > 1000 and r.content[:2] == b"PK":
            data_stream = BytesIO(r.content)

            minio_client.put_object(
                bucket_name=MINIO_BUCKET,
                object_name=object_name,
                data=data_stream,
                length=len(r.content),
                content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

            return f"✅ {ts} uploaded ({len(r.content)} bytes)"

        return f"❌ {ts} missing or invalid content"

    except Exception as e:
        return f"⚠️ {ts} error: {e}"


def download(max_workers=10):
    timestamps = list(generate_timestamps(start_date, end_date))

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(download_file, ts): ts for ts in timestamps}

        for future in tqdm(as_completed(futures), total=len(futures), desc="Uploading"):
            logger.info(future.result())

    logger.info("✔ Finished downloading + uploading all files.")


if __name__ == "__main__":
    download()
    exit(0)
