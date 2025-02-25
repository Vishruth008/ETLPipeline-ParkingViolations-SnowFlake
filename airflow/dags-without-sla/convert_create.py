from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils.dates import days_ago
from datetime import datetime
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import io
import os
import gzip
import time

# Constants
S3_BUCKET = 'parking-violations-bucket'
CSV_FILE_PATH = 'Parking_Violations_Issued_Fiscal_Year_2024.csv.gz'
PARQUET_FILE_PATH = 'Parking_Violations_Issued_Fiscal_Year_2024.parquet'

# Snowflake environment variables
SNOWFLAKE_WAREHOUSE = 'PARKING_WAREHOUSE'
SNOWFLAKE_DB = 'PARKING_DB'
SNOWFLAKE_SCHEMA = 'PARKING_SCHEMA'
SNOWFLAKE_TABLE_PARKING_VIOLATIONS = 'PARKING_VIOLATIONS'
SNOWFLAKE_TABLE_PARKING_VIOLATIONS_CODE = 'PARKING_VIOLATIONS_CODE'
SNOWFLAKE_STAGE = 'PARKING_STAGE'
STORAGE_INTEGRATION_NAME = 'PARKING_INTEGRATION'
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::767397772312:role/parking-de-snowflake'

# Define default args for DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(2),
    'retries': 1,
}

# Define the DAG
dag = DAG(
    'parking_violations_convert_create',
    default_args=default_args,
    description='Convert CSV to Parquet and Create Snowflake Schema',
    schedule_interval='@once',
)

# Task to convert CSV to Parquet directly in S3
def convert_csv_to_parquet():
    start_time = time.time()
    print("Starting the CSV to Parquet conversion process")

    s3_hook = S3Hook(aws_conn_id='aws_default')

    # Download gzipped CSV file content from S3
    print(f"Downloading gzipped CSV content from S3 bucket {S3_BUCKET}, key {CSV_FILE_PATH}")
    gzipped_csv_content = s3_hook.get_key(key=CSV_FILE_PATH, bucket_name=S3_BUCKET).get()["Body"].read()
    
    # Decompress gzipped CSV content
    print("Decompressing gzipped CSV content")
    with gzip.GzipFile(fileobj=io.BytesIO(gzipped_csv_content), mode='rb') as gz:
        csv_content = gz.read().decode('utf-8')
    
    # Read CSV content into DataFrame in chunks
    print("Reading CSV content into DataFrame in chunks")
    chunk_size = 10000  # Adjust the chunk size based on available memory
    parquet_buffer = io.BytesIO()
    
    parquet_writer = None
    schema = None

    for i, chunk in enumerate(pd.read_csv(io.StringIO(csv_content), chunksize=chunk_size, dtype=str)):
        print(f"Processing chunk {i+1}")
        
        # Handle null values and adjust column names
        chunk = chunk.fillna("N/A")
        chunk.columns = [col.upper().replace(' ', '_') for col in chunk.columns]
        chunk.columns = [col.upper().replace('?', '') for col in chunk.columns]

        if i == 0:
            schema = pa.Schema.from_pandas(df=chunk)
            parquet_writer = pq.ParquetWriter(parquet_buffer, schema, compression='gzip')
        table = pa.Table.from_pandas(chunk, schema=schema)
        parquet_writer.write_table(table)
    
    if parquet_writer:
        parquet_writer.close()
    
    parquet_buffer.seek(0)
    print("Parquet file created in memory")

    # Upload Parquet file content to S3
    print(f"Uploading Parquet file to S3 bucket {S3_BUCKET}, key {PARQUET_FILE_PATH}")
    s3_hook.load_bytes(parquet_buffer.read(), key=PARQUET_FILE_PATH, bucket_name=S3_BUCKET, replace=True)
    print(f"Uploaded Parquet file to S3 bucket {S3_BUCKET}")

    end_time = time.time()
    duration = (end_time - start_time) / 60
    print(f"CSV to Parquet conversion completed in {duration:.2f} minutes")

convert_task = PythonOperator(
    task_id='convert_csv_to_parquet',
    python_callable=convert_csv_to_parquet,
    dag=dag,
)

# Task to load data into Snowflake from S3
def create_snowflake_schema_and_integration():
    start_time = time.time()
    print("Starting the create_snowflake_schema_and_integration task...")

    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    conn = snowflake_hook.get_conn()
    cursor = conn.cursor()
    print("Snowflake connection established.")

    cursor.execute(f"CREATE WAREHOUSE IF NOT EXISTS {SNOWFLAKE_WAREHOUSE} WITH WAREHOUSE_SIZE='x-small'")
    print(f"Warehouse {SNOWFLAKE_WAREHOUSE} created/exists.")
    
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {SNOWFLAKE_DB}")
    print(f"Database {SNOWFLAKE_DB} created/exists.")
    
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {SNOWFLAKE_SCHEMA}")
    print(f"Schema {SNOWFLAKE_SCHEMA} created/exists.")

    cursor.execute(f"USE DATABASE {SNOWFLAKE_DB}")
    cursor.execute(f"USE SCHEMA {SNOWFLAKE_SCHEMA}")
    print(f"Using database {SNOWFLAKE_DB} and schema {SNOWFLAKE_SCHEMA}.")

    cursor.execute(f"""
    CREATE OR REPLACE STORAGE INTEGRATION {STORAGE_INTEGRATION_NAME}
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = S3
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = '{STORAGE_AWS_ROLE_ARN}'
    STORAGE_ALLOWED_LOCATIONS = ('s3://{S3_BUCKET}');
    """)
    print(f"Storage integration {STORAGE_INTEGRATION_NAME} created with role {STORAGE_AWS_ROLE_ARN} and bucket {S3_BUCKET}.")

    cursor.execute(f"""
    CREATE OR REPLACE STAGE {SNOWFLAKE_STAGE}
    URL='s3://{S3_BUCKET}'
    STORAGE_INTEGRATION = {STORAGE_INTEGRATION_NAME}
    """)
    print(f"Stage {SNOWFLAKE_STAGE} created pointing to S3 bucket {S3_BUCKET}.")

    conn.close()
    print("Snowflake connection closed.")

    print("Warehouse, database, schema, storage integration, and stage created successfully in Snowflake.")
    
    end_time = time.time()
    duration = (end_time - start_time) / 60
    print(f"Snowflake schema and integration creation completed in {duration:.2f} minutes")

create_task = PythonOperator(
    task_id='create_schema',
    python_callable=create_snowflake_schema_and_integration,
    dag=dag,
)

# Set task dependencies
convert_task >> create_task
