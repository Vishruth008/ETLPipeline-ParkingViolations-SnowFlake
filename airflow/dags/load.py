from airflow import DAG, Dataset
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils.dates import days_ago
from airflow.utils.email import send_email
from airflow.models import Variable
from datetime import datetime, timedelta
import pandas as pd
import io
import time
import re
import os

# Constants
S3_BUCKET = 'parking-violations-bucket'
CSV_FILE_PATH_VIOLATION_CODE = 'DOF_Parking_Violation_Codes.csv'
_CSV_FILE_NAME_VIOLATION_ = Variable.get("parking_violations_file_name")
CSV_FILE_NAME_VIOLATION = _CSV_FILE_NAME_VIOLATION_  + '.csv'
CSV_FILE_PATH_VIOLATION = CSV_FILE_NAME_VIOLATION + '.gz'
PARQUET_FILE_PATH_VIOLATION = _CSV_FILE_NAME_VIOLATION_ + '.parquet'

testing_cols='testing_cols'

# Ensure data folder exists
os.makedirs(testing_cols, exist_ok=True)

local_testing_cols = os.path.join(testing_cols, _CSV_FILE_NAME_VIOLATION_)

year = re.search(r'Year_(\d{4})', _CSV_FILE_NAME_VIOLATION_).group(1)

# Snowflake environment variables
SNOWFLAKE_WAREHOUSE = 'PARKING_WAREHOUSE'
SNOWFLAKE_DB = 'PARKING_DB'
SNOWFLAKE_SCHEMA = 'PARKING_SCHEMA'
SNOWFLAKE_TABLE_PARKING_VIOLATIONS = 'PARKING_VIOLATIONS_' + str(year)
SNOWFLAKE_TABLE_PARKING_VIOLATIONS_CODE = 'PARKING_VIOLATIONS_CODE'
SNOWFLAKE_STAGE = 'PARKING_STAGE'
STORAGE_INTEGRATION_NAME = 'PARKING_INTEGRATION'
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::767397772312:role/parking-de-snowflake'

D1 = Dataset(f"s3://{S3_BUCKET}/{CSV_FILE_PATH_VIOLATION_CODE}")
D3 = Dataset(f"s3://{S3_BUCKET}/{PARQUET_FILE_PATH_VIOLATION}")

# Once AWS External Id is connected then change schedule to D3
# schedule=[D3]
# schedule_interval=None

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
    'sla': timedelta(minutes=15),  # DAG-level SLA
}

# Define the DAG
dag = DAG(
    'parking_violations_load_data_to_snowflake-3',
    default_args=default_args,
    description='Load data into Snowflake from S3',
    schedule=[D3],
    catchup=False,
    sla_miss_callback=sla_miss_email,
    tags=['s3', 'snowflake', 'load'],
    doc_md=f"""
    ### Parking Violations Load Data to Snowflake
    This DAG loads parking violations data for the year {year} from S3 into Snowflake, ensuring the necessary tables and stages are created and data is copied efficiently.
    """
)

def read_parquet_from_s3(s3_key):
    s3_hook = S3Hook(aws_conn_id='aws_default')
    print("S3Hook initialized.")
    parquet_content = s3_hook.get_key(key=s3_key, bucket_name=S3_BUCKET).get()
    parquet_data = parquet_content['Body'].read()
    return pd.read_parquet(io.BytesIO(parquet_data))

def read_csv_from_s3(s3_key):
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
    print(f"Reading Parquet file {PARQUET_FILE_PATH_VIOLATION} from S3 bucket {S3_BUCKET}...")
    df_parking_violations = read_parquet_from_s3(PARQUET_FILE_PATH_VIOLATION)
    
    # For Testing Purposes: Save column names to CSV
    column_names_df = pd.DataFrame(df_parking_violations.columns, columns=['Column Names'])
    column_names_df.to_csv(f'{local_testing_cols}.csv', index=False)
    print(f"Column names saved to {local_testing_cols}.csv")

    print("Parquet file read into DataFrame.")
    print(df_parking_violations.head())

    create_table_from_df(df_parking_violations, SNOWFLAKE_TABLE_PARKING_VIOLATIONS)

    copy_query_parking_violations = f"""
    COPY INTO {SNOWFLAKE_TABLE_PARKING_VIOLATIONS}
    FROM @{SNOWFLAKE_STAGE}/{PARQUET_FILE_PATH_VIOLATION}
    FILE_FORMAT = (TYPE = PARQUET)
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
    """
    cursor.execute(copy_query_parking_violations)
    print(f"Data copied into {SNOWFLAKE_TABLE_PARKING_VIOLATIONS} from S3 stage {SNOWFLAKE_STAGE}.")

    # Read and load parking violation codes data
    print(f"Reading CSV file {CSV_FILE_PATH_VIOLATION_CODE} from S3 bucket {S3_BUCKET}...")
    df_parking_violations_code = read_csv_from_s3(CSV_FILE_PATH_VIOLATION_CODE)
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
        key=CSV_FILE_PATH_VIOLATION_CODE,
        bucket_name=S3_BUCKET,
        replace=True
    )
    print(f"Modified CSV file uploaded back to S3 bucket {S3_BUCKET}.")

    create_table_from_df(df_parking_violations_code, SNOWFLAKE_TABLE_PARKING_VIOLATIONS_CODE)

    copy_query_parking_violations_code = f"""
    COPY INTO {SNOWFLAKE_TABLE_PARKING_VIOLATIONS_CODE}
    FROM @{SNOWFLAKE_STAGE}/{CSV_FILE_PATH_VIOLATION_CODE}
    FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = ',' 
        SKIP_HEADER = 1 
        NULL_IF = ('NULL', 'null') 
        EMPTY_FIELD_AS_NULL = TRUE)
    """
    cursor.execute(copy_query_parking_violations_code)
    print(f"Data copied into {SNOWFLAKE_TABLE_PARKING_VIOLATIONS_CODE} from S3 stage {SNOWFLAKE_STAGE}.")

    # Remove files from S3
    s3_hook.delete_objects(bucket=S3_BUCKET, keys=[PARQUET_FILE_PATH_VIOLATION, CSV_FILE_PATH_VIOLATION_CODE])
    print(f"Files {PARQUET_FILE_PATH_VIOLATION} and {CSV_FILE_PATH_VIOLATION_CODE} removed from S3 bucket {S3_BUCKET}.")

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
    sla=timedelta(minutes=10),  # Task-level SLA
    on_success_callback=success_email,
    on_failure_callback=failure_email,
    outlets=[D1, D3],
    doc_md=f"""
    ### Load Data to Snowflake
    This task reads parking violations data for the year {year} from Parquet and CSV files in S3, creates the necessary tables in Snowflake, and loads the data into Snowflake tables.
    """
)

# Set task dependencies
load_data_task
