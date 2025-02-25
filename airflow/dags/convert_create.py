from airflow import DAG, Dataset
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils.dates import days_ago
from airflow.utils.email import send_email
from airflow.models import Variable
from datetime import timedelta
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import io
import gzip
import time
import re

# Constants
S3_BUCKET = 'parking-violations-bucket'
CSV_FILE_PATH_VIOLATION_CODE = 'DOF_Parking_Violation_Codes.csv'
_CSV_FILE_NAME_VIOLATION_ = Variable.get("parking_violations_file_name")
CSV_FILE_NAME_VIOLATION = _CSV_FILE_NAME_VIOLATION_  + '.csv'
CSV_FILE_PATH_VIOLATION = CSV_FILE_NAME_VIOLATION + '.gz'
PARQUET_FILE_PATH_VIOLATION = _CSV_FILE_NAME_VIOLATION_ + '.parquet'

year = re.search(r'Year_(\d{4})', _CSV_FILE_NAME_VIOLATION_).group(1)

# Snowflake environment variables
SNOWFLAKE_WAREHOUSE = 'PARKING_WAREHOUSE'
SNOWFLAKE_DB = 'PARKING_DB'
SNOWFLAKE_SCHEMA = 'PARKING_SCHEMA'
SNOWFLAKE_STAGE = 'PARKING_STAGE'
STORAGE_INTEGRATION_NAME = 'PARKING_INTEGRATION'
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::767397772312:role/parking-de-snowflake'

D1 = Dataset(f"s3://{S3_BUCKET}/{CSV_FILE_PATH_VIOLATION_CODE}")
D2 = Dataset(f"s3://{S3_BUCKET}/{CSV_FILE_PATH_VIOLATION}")
D3 = Dataset(f"s3://{S3_BUCKET}/{PARQUET_FILE_PATH_VIOLATION}")

# Email alert functions
def send_alert_email(task_id, task_status, execution_date, log_url, execution_time, task_desc, to_email):
    subject = f'Airflow Task {task_id} {task_status} for the year {year}'
    body = f"""
    <html> 
    <head> 
    <body><br>  
    Hi User <br> 
    The task <b>{task_id}</b> finished with status: <b>{task_status}</b> <br><br> 
    Task execution date: {execution_date} <br> 
    Execution time: {execution_time:.2f} minutes<br> 
    Task description: {task_desc}<br><br>
    <p>Log URL: {log_url} <br> 
    Regards,<br> 
    BS
    </p>
    </body> 
    </head> 
    </html>
    """
    send_email(to=to_email, subject=subject, html_content=body)

def success_email(context):
    task_instance = context['task_instance']
    execution_time = (task_instance.end_date - task_instance.start_date).total_seconds() / 60
    task_desc = task_instance.task.doc_md or "No description provided."
    send_alert_email(
        task_instance.task_id,
        'Success',
        context["data_interval_start"],
        task_instance.log_url,
        execution_time,
        task_desc,
        'badreeshshetty@gmail.com'
    )

def failure_email(context):
    task_instance = context['task_instance']
    execution_time = (task_instance.end_date - task_instance.start_date).total_seconds() / 60
    task_desc = task_instance.task.doc_md or "No description provided."
    send_alert_email(
        task_instance.task_id,
        'Failed',
        context["data_interval_start"],
        task_instance.log_url,
        execution_time,
        task_desc,
        'badreeshshetty@gmail.com'
    )

def sla_miss_email(dag, task_list, blocking_task_list, slas, blocking_tis):
    subject = f"Airflow SLA Miss: {dag.dag_id}"
    html_content = f"""
    <p>For the year: {year}</p>
    <p>SLA Miss for DAG: {dag.dag_id}</p>
    <p>Tasks that missed their SLA: {', '.join(task.task_id for task in task_list)}</p>
    <p>Blocking tasks: {', '.join(task.task_id for task in blocking_task_list)}</p>
    """
    print(f"SLA Miss detected for DAG {dag.dag_id}. Sending email...")
    send_email(to=['badreeshshetty@gmail.com'], subject=subject, html_content=html_content)
    print("SLA Miss email sent.")

# Define default args for DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
    'email_on_retry': False,
    'depends_on_past': False,
    'retry_delay': timedelta(seconds=5),
    'sla': timedelta(minutes=35),  # DAG-level SLA
}

# Define the DAG
dag2 = DAG(
    'parking_violations_convert_create-2',
    default_args=default_args,
    description='Convert CSV to Parquet and Create Snowflake Schema',
    schedule=[D2],
    catchup=False,
    sla_miss_callback=sla_miss_email,
    tags=['s3', 'csv-parquet', 'snowflake', 'extract', 'raw-transformations'],
    doc_md=f"""
    ### Parking Violations Convert and Create
    This DAG converts parking violations data for the year {year} from CSV to Parquet and creates the necessary schema and integration in Snowflake.
    """
)

# Task to convert CSV to Parquet directly in S3
def convert_csv_to_parquet():
    start_time = time.time()
    print("Starting the CSV to Parquet conversion process")

    s3_hook = S3Hook(aws_conn_id='aws_default')

    # Download gzipped CSV file content from S3
    print(f"Downloading gzipped CSV content from S3 bucket {S3_BUCKET}, key {CSV_FILE_PATH_VIOLATION}")
    gzipped_csv_content = s3_hook.get_key(key=CSV_FILE_PATH_VIOLATION, bucket_name=S3_BUCKET).get()["Body"].read()
    
    # Decompress gzipped CSV content
    print("Decompressing gzipped CSV content")
    with gzip.GzipFile(fileobj=io.BytesIO(gzipped_csv_content), mode='rb') as gz:
        csv_content = gz.read().decode('utf-8')

    # Read CSV content into DataFrame in chunks
    print("Reading CSV content into DataFrame in chunks")
    chunk_size = 5000
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
    print(f"Uploading Parquet file to S3 bucket {S3_BUCKET}, key {PARQUET_FILE_PATH_VIOLATION}")
    s3_hook.load_bytes(parquet_buffer.read(), key=PARQUET_FILE_PATH_VIOLATION, bucket_name=S3_BUCKET, replace=True)
    print(f"Uploaded Parquet file to S3 bucket {S3_BUCKET}")

    # Remove files from S3
    s3_hook.delete_objects(bucket=S3_BUCKET, keys=CSV_FILE_PATH_VIOLATION)
    print(f"Files {CSV_FILE_PATH_VIOLATION} removed from S3 bucket {S3_BUCKET}.")

    end_time = time.time()
    duration = (end_time - start_time) / 60
    print(f"CSV to Parquet conversion completed in {duration:.2f} minutes")



convert_task = PythonOperator(
    task_id='convert_csv_to_parquet',
    python_callable=convert_csv_to_parquet,
    dag=dag2,
    sla=timedelta(minutes=30),  # Task-level SLA
    on_success_callback=success_email,
    on_failure_callback=failure_email,
    outlets=[D3],
    doc_md=f"""
    ### Convert CSV to Parquet
    This task downloads a gzipped CSV file from S3, converts it to Parquet format in memory, and uploads the Parquet file back to S3 for the year {year}.
    """
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
    CREATE STORAGE INTEGRATION IF NOT EXISTS {STORAGE_INTEGRATION_NAME}
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = S3
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = '{STORAGE_AWS_ROLE_ARN}'
    STORAGE_ALLOWED_LOCATIONS = ('s3://{S3_BUCKET}');
    """)
    print(f"Storage integration {STORAGE_INTEGRATION_NAME} created with role {STORAGE_AWS_ROLE_ARN} and bucket {S3_BUCKET}.")

    cursor.execute(f"""
    CREATE STAGE IF NOT EXISTS {SNOWFLAKE_STAGE}
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
    dag=dag2,
    sla=timedelta(minutes=2),  # Task-level SLA
    on_success_callback=success_email,
    on_failure_callback=failure_email,
    doc_md=f"""
    ### Create Snowflake Schema and Integration
    This task creates a Snowflake warehouse, database, schema, storage integration, and stage to facilitate data loading from S3 for the year {year}.
    """
)

# Set task dependencies
convert_task >> create_task
