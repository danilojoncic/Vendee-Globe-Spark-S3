import logging
from io import BytesIO
from tqdm import tqdm
import pandas as pd
from minio import Minio
from minio.error import S3Error
from concurrent.futures import ThreadPoolExecutor, as_completed


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)


MINIO_ENDPOINT = "nginx:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"
MINIO_BUCKET = "raw"
MINIO_OUTPUT_BUCKET = "csv"

minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

if not minio_client.bucket_exists(MINIO_OUTPUT_BUCKET):
    minio_client.make_bucket(MINIO_OUTPUT_BUCKET)



def list_xlsx_files():
    try:
        objects = minio_client.list_objects(MINIO_BUCKET)
        xlsx_files = [obj.object_name for obj in objects if obj.object_name.endswith(".xlsx")]
        logger.info(f"Found {len(xlsx_files)} Excel files in {MINIO_BUCKET}")
        return xlsx_files
    except S3Error as e:
        logger.error(f"Error listing objects from MinIO: {e}")
        return []


def process_file(file_name: str):
    try:
        response = minio_client.get_object(MINIO_BUCKET, file_name)
        data = response.read()
        df_check = pd.read_excel(BytesIO(data), engine="calamine", skiprows=3, nrows=1)

        if any('arrivée' in str(col).lower() or 'arrival date' in str(col).lower() for col in df_check.columns):
            logger.info(f"Detected ARV format in {file_name}, searching for racing table...")
            df_full = pd.read_excel(BytesIO(data), engine="calamine", header=None)
            beginning_of_racing_rows = 3
            for idx in range(len(df_full)):
                row_values = df_full.iloc[idx].astype(str).str.cat(sep=' ')
                if 'Depuis 30 minutes' in row_values or 'Since 30 minutes' in row_values:
                    beginning_of_racing_rows = idx
                    break
        else:
            beginning_of_racing_rows = 3

        df = pd.read_excel(BytesIO(data), engine="calamine", skiprows=beginning_of_racing_rows)
        df = df.iloc[:, 1:]
        df = df.iloc[:-4, :]
        df = df.drop(0, errors="ignore")
        df.columns = df.columns.str.replace(r'[\r\n]+', ' ', regex=True).str.strip()
        df = df.map(lambda x: str(x).replace("\r\n", " ").strip() if isinstance(x, str) else x)
        first_col = df.columns[0]
        df = df[~df[first_col].astype(str).str.strip().isin(['RET', 'DNF', 'ARV'])]
        df = df.reset_index(drop=True)

        new_columns = [
            'rank',
            'nationality_sail',
            'skipper_boat',
            'time',
            'latitude',
            'longitude',
            'heading_30min',
            'speed_30min',
            'avg_speed_30min',
            'distance_30min',
            'heading_last_report',
            'speed_last_report',
            'avg_speed_last_report',
            'distance_last_report',
            'heading_24h',
            'speed_24h',
            'avg_speed_24h',
            'distance_24h',
            'dtf',
            'dtl'
        ]

        actual_cols = len(df.columns)
        if actual_cols != len(new_columns):
            logger.warning(f"Expected {len(new_columns)} columns but found {actual_cols}")
            new_columns = new_columns[:actual_cols]

        df.columns = new_columns

        csv_buffer = BytesIO()
        df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)
        csv_name = file_name.replace(".xlsx", ".csv")

        minio_client.put_object(
            bucket_name=MINIO_OUTPUT_BUCKET,
            object_name=csv_name,
            data=csv_buffer,
            length=csv_buffer.getbuffer().nbytes,
            content_type="text/csv"
        )
        logger.info(f"Processed {file_name} -> {csv_name}")
        return file_name, True

    except Exception as e:
        logger.error(f"Failed to process {file_name}: {e}")
        return file_name, False

def convert_all_xlsx_to_csv_concurrent(max_workers=10):
    xlsx_files = list_xlsx_files()
    if not xlsx_files:
        return

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(process_file, file_name): file_name for file_name in xlsx_files}
        for future in tqdm(as_completed(futures), total=len(futures), desc="Processing Excel files"):
            file_name, success = future.result()
            if not success:
                logger.warning(f"File {file_name} failed to process.")


def check_csv_bucket_complete(expected_count=696):
    try:
        if not minio_client.bucket_exists(MINIO_OUTPUT_BUCKET):
            return False

        objects = list(minio_client.list_objects(MINIO_OUTPUT_BUCKET))
        if len(objects) >= expected_count:
            logger.info(
                f"Output bucket '{MINIO_OUTPUT_BUCKET}' already has {len(objects)} files. Exiting."
            )
            return True
        return False
    except Exception as e:
        logger.error(f"Failed to check output bucket: {e}")
        return False


if __name__ == "__main__":
    if check_csv_bucket_complete():
        exit(0)

    convert_all_xlsx_to_csv_concurrent(max_workers=10)
