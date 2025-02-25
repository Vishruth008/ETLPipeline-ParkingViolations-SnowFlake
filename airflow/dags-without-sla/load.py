from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils.dates import days_ago
import pandas as pd
import io
import os
import time
import re

# Constants
S3_BUCKET = 'parking-violations-bucket'
CSV_FILE_PATH = 'Parking_Violations_Issued_Fiscal_Year_2024.csv.gz'
PARQUET_FILE_PATH = 'Parking_Violations_Issued_Fiscal_Year_2024.parquet'
PARKING_VIOLATIONS_CODE_PATH = 'DOF_Parking_Violation_Codes.csv'

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
    'parking_violations_load_data_to_snowflake',
    default_args=default_args,
    description='Load data into Snowflake from S3',
    schedule_interval='@once',
)

def read_parquet_from_s3(s3_key):
    # Initialize S3Hook
    s3_hook = S3Hook(aws_conn_id='aws_default')
    print("S3Hook initialized.")
    parquet_content = s3_hook.get_key(key=s3_key, bucket_name=S3_BUCKET).get()
    parquet_data = parquet_content['Body'].read()
    return pd.read_parquet(io.BytesIO(parquet_data))

def read_csv_from_s3(s3_key):
    # Initialize S3Hook
    s3_hook = S3Hook(aws_conn_id='aws_default')
    print("S3Hook initialized.")
    csv_content = s3_hook.get_key(key=s3_key, bucket_name=S3_BUCKET).get()
    csv_data = csv_content['Body'].read()
    return pd.read_csv(io.BytesIO(csv_data))

def create_table_from_df(df, table_name):
    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    conn = snowflake_hook.get_conn()
    cursor = conn.cursor()
    type_mapping = {
        'object': 'STRING',
        'int64': 'NUMBER',
        'float64': 'FLOAT',
        'bool': 'BOOLEAN',
        'datetime64[ns]': 'TIMESTAMP'
    }
    columns = ', '.join([f"{col} {type_mapping[str(dtype)]}" for col, dtype in df.dtypes.items()])
    create_table_query = f"""
    CREATE OR REPLACE TABLE {table_name} (
        {columns}
    )
    """
    cursor.execute(create_table_query)
    print(f"Table {table_name} created in Snowflake.")

def load_data_to_snowflake():
    start_time = time.time()
    print("Starting the load_data_to_snowflake task...")

    # Initialize S3Hook
    s3_hook = S3Hook(aws_conn_id='aws_default')
    print("S3Hook initialized.")

    # Establish a connection to Snowflake
    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    conn = snowflake_hook.get_conn()
    cursor = conn.cursor()
    print("Snowflake connection established.")

    # Use the specified database and schema
    cursor.execute(f"USE DATABASE {SNOWFLAKE_DB}")
    cursor.execute(f"USE SCHEMA {SNOWFLAKE_SCHEMA}")
    print(f"Using database {SNOWFLAKE_DB} and schema {SNOWFLAKE_SCHEMA}.")

    # Read and load parking violations data
    print(f"Reading Parquet file {PARQUET_FILE_PATH} from S3 bucket {S3_BUCKET}...")
    df_parking_violations = read_parquet_from_s3(PARQUET_FILE_PATH)
    print("Parquet file read into DataFrame.")
    print(df_parking_violations.head())

    create_table_from_df(df_parking_violations, SNOWFLAKE_TABLE_PARKING_VIOLATIONS)

    copy_query_parking_violations = f"""
    COPY INTO {SNOWFLAKE_TABLE_PARKING_VIOLATIONS}
    FROM @{SNOWFLAKE_STAGE}/{PARQUET_FILE_PATH}
    FILE_FORMAT = (TYPE = PARQUET)
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
    """
    cursor.execute(copy_query_parking_violations)
    print(f"Data copied into {SNOWFLAKE_TABLE_PARKING_VIOLATIONS} from S3 stage {SNOWFLAKE_STAGE}.")

    # Read and load parking violation codes data
    print(f"Reading CSV file {PARKING_VIOLATIONS_CODE_PATH} from S3 bucket {S3_BUCKET}...")
    df_parking_violations_code = read_csv_from_s3(PARKING_VIOLATIONS_CODE_PATH)
    print("CSV file read into DataFrame.")
    print(df_parking_violations_code.head())

    # Modify column names
    df_parking_violations_code.columns = [col.upper().replace(' ', '_') for col in df_parking_violations_code.columns]
    df_parking_violations_code.columns = [re.sub(r'\W+', '', col).upper() for col in df_parking_violations_code.columns]

    # Save the modified DataFrame back to CSV and upload to S3
    modified_csv_buffer = io.StringIO()
    df_parking_violations_code.to_csv(modified_csv_buffer, index=False)
    s3_hook.load_string(
        string_data=modified_csv_buffer.getvalue(),
        key=PARKING_VIOLATIONS_CODE_PATH,
        bucket_name=S3_BUCKET,
        replace=True
    )
    print(f"Modified CSV file uploaded back to S3 bucket {S3_BUCKET}.")

    create_table_from_df(df_parking_violations_code, SNOWFLAKE_TABLE_PARKING_VIOLATIONS_CODE)

    copy_query_parking_violations_code = f"""
    COPY INTO {SNOWFLAKE_TABLE_PARKING_VIOLATIONS_CODE}
    FROM @{SNOWFLAKE_STAGE}/{PARKING_VIOLATIONS_CODE_PATH}
    FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = ',' 
        SKIP_HEADER = 1 
        NULL_IF = ('NULL', 'null') 
        EMPTY_FIELD_AS_NULL = TRUE)
    """
    cursor.execute(copy_query_parking_violations_code)
    print(f"Data copied into {SNOWFLAKE_TABLE_PARKING_VIOLATIONS_CODE} from S3 stage {SNOWFLAKE_STAGE}.")

    # Close the connection
    conn.close()
    print("Snowflake connection closed.")

    end_time = time.time()
    duration = (end_time - start_time) / 60
    print(f"Data loaded successfully to Snowflake in {duration:.2f} minutes.")

load_data_task = PythonOperator(
    task_id='load_data_to_snowflake',
    python_callable=load_data_to_snowflake,
    dag=dag,
)

load_data_task
