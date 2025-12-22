import os
import boto3
import snowflake.connector
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# -----------------------
# MinIO configuration
# -----------------------
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password123"
BUCKET = "bronze-transactions"
LOCAL_DIR = "/tmp/minio_downloads"  # absolute path

# -----------------------
# Snowflake configuration
# -----------------------
SNOWFLAKE_USER = "snowflake_user"
SNOWFLAKE_PASSWORD = "password"
SNOWFLAKE_ACCOUNT = "SNOWFLAKE_ACCOUNT"
SNOWFLAKE_WAREHOUSE = "COMPUTE_WH"
SNOWFLAKE_DB = "stock_mds"
SNOWFLAKE_SCHEMA = "common"

# -----------------------
# Download files from MinIO
# -----------------------
def download_from_minio():
    os.makedirs(LOCAL_DIR, exist_ok=True)
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY
    )

    # List all objects in the bucket
    objects = s3.list_objects_v2(Bucket=BUCKET).get("Contents", [])
    if not objects:
        print("No objects found in the bucket.")
        return []

    local_files = []
    for obj in objects:
        key = obj["Key"]  # e.g., "AAPL/file.json"
        local_file = os.path.join(LOCAL_DIR, key)  # preserve folder structure
        os.makedirs(os.path.dirname(local_file), exist_ok=True)
        s3.download_file(BUCKET, key, local_file)
        print(f"Downloaded {key} -> {local_file}")
        local_files.append(local_file)

    return local_files

# -----------------------
# Load files into Snowflake
# -----------------------
def load_to_snowflake(**kwargs):
    ti = kwargs['ti']
    local_files = ti.xcom_pull(task_ids='download_minio') or []

    if not local_files:
        print("No files to load.")
        return

    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DB,
        schema=SNOWFLAKE_SCHEMA
    )
    cur = conn.cursor()

    for f in local_files:
        cur.execute(f"PUT file://{f} @%bronze_stock_quotes_raw")
        print(f"Uploaded {f} to Snowflake stage")
        os.remove(f)
        print(f"Deleted local file {f}")

    cur.execute("""
        COPY INTO bronze_stock_quotes_raw
        FROM @%bronze_stock_quotes_raw
        FILE_FORMAT = (TYPE=JSON)
    """)
    print("COPY INTO executed")

    cur.close()
    conn.close()

# -----------------------
# Default DAG arguments
# -----------------------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 12, 20),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# -----------------------
# Define the DAG
# -----------------------
with DAG(
    "minio_to_snowflake_storage_safe",
    default_args=default_args,
    schedule_interval="*/1 * * * *",  # every 1 minute
    catchup=False,
) as dag:

    task1 = PythonOperator(
        task_id="download_minio",
        python_callable=download_from_minio,
    )

    task2 = PythonOperator(
        task_id="load_snowflake",
        python_callable=load_to_snowflake,
        provide_context=True,
    )

    task1 >> task2
