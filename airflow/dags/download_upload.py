from airflow import DAG, Dataset
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago
from airflow.utils.email import send_email
from airflow.models import Variable
from datetime import timedelta
import os
import requests
import gzip
import shutil
import time
import re 

# Constants
S3_BUCKET = 'parking-violations-bucket'
DATA_FOLDER = 'data'
CSV_FILE_PATH_VIOLATION_CODE = 'DOF_Parking_Violation_Codes.csv'
VIOLATION_CODE_URL = 'https://data.cityofnewyork.us/api/views/ncbg-6agr/rows.csv?accessType=DOWNLOAD'
_CSV_FILE_NAME_VIOLATION_ = Variable.get("parking_violations_file_name")
VIOLATION_URL = Variable.get("parking_violations_url")
CSV_FILE_NAME_VIOLATION = _CSV_FILE_NAME_VIOLATION_  + '.csv'
CSV_FILE_PATH_VIOLATION = CSV_FILE_NAME_VIOLATION + '.gz'

year = re.search(r'Year_(\d{4})', _CSV_FILE_NAME_VIOLATION_).group(1)

D1 = Dataset(f"s3://{S3_BUCKET}/{CSV_FILE_PATH_VIOLATION_CODE}")
D2 = Dataset(f"s3://{S3_BUCKET}/{CSV_FILE_PATH_VIOLATION}")

# Ensure data folder exists
os.makedirs(DATA_FOLDER, exist_ok=True)

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

# Default args for DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
    'email_on_retry': False,
    'depends_on_past': False,
    'retry_delay': timedelta(seconds=5),
    'sla': timedelta(minutes=30),  # DAG-level SLA
}

# Define the DAG
dag1 = DAG(
    'parking_violations_download_upload-1',
    default_args=default_args,
    description=f'Download and upload parking violations data to S3 for year {year}',
    schedule_interval='@once',
    catchup=False,
    sla_miss_callback=sla_miss_email,
    tags=['requests','s3','extract','raw'],
    doc_md=f"""
    ### Parking Violations Download and Upload
    This DAG downloads parking violations data from NYC Open Data and uploads it to an S3 bucket. 
    The data includes parking violation codes and issued parking violations for the fiscal year {year}.
    """
)

# Task to download CSV files locally
def download_csv_files():
    """
    ### Download CSV Files
    This task downloads the parking violations data from NYC Open Data.
    """
    print("Download task started.")
    start_time = time.time()
    
    urls = {
       CSV_FILE_PATH_VIOLATION_CODE: VIOLATION_CODE_URL,
       CSV_FILE_NAME_VIOLATION: VIOLATION_URL
    }
    
    for filename, url in urls.items():
        local_file_path = os.path.join(DATA_FOLDER, filename)
        response = requests.get(url)
        with open(local_file_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)
        print(f"Downloaded {filename} locally at {local_file_path}")
        
        if filename == CSV_FILE_NAME_VIOLATION:
            gzip_file_path = local_file_path + '.gz'
            with open(local_file_path, 'rb') as f_in, gzip.open(gzip_file_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
            print(f"Compressed {filename} to {gzip_file_path}")
            os.remove(local_file_path)
            print(f"Removed original file {local_file_path}")
    
    end_time = time.time()
    duration = (end_time - start_time) / 60
    print(f"Download task completed in {duration:.2f} minutes")

download_task = PythonOperator(
    task_id='download_csv_files',
    python_callable=download_csv_files,
    dag=dag1,
    sla=timedelta(minutes=20),
    on_success_callback=success_email,
    on_failure_callback=failure_email,
    doc_md=f"""
    ### Download CSV Files
    This task downloads the parking violations data from NYC Open Data for the year {year}.
    """
)

# Task to upload downloaded files to S3
def upload_files_to_s3():
    """
    ### Upload Files to S3
    This task uploads the downloaded parking violations data to an S3 bucket.
    """
    print("Upload task started.")
    start_time = time.time()
    
    s3_hook = S3Hook(aws_conn_id='aws_default')
    
    for filename in os.listdir(DATA_FOLDER):
        local_file_path = os.path.join(DATA_FOLDER, filename)
        if os.path.isfile(local_file_path):
            s3_hook.load_file(local_file_path, key=filename, bucket_name=S3_BUCKET, replace=True)
            print(f"Uploaded {filename} to S3 bucket {S3_BUCKET}")
            # Remove file from local data folder
            os.remove(local_file_path)
            print(f"Removed {filename} from local data folder")
    
    end_time = time.time()
    duration = (end_time - start_time) / 60
    print(f"Upload task completed in {duration:.2f} minutes")

upload_task = PythonOperator(
    task_id='upload_files_to_s3',
    python_callable=upload_files_to_s3,
    dag=dag1,
    sla=timedelta(minutes=5),
    on_success_callback=success_email,
    on_failure_callback=failure_email,
    outlets=[D1, D2],
    doc_md=f"""
    ### Upload Files to S3
    This task uploads the downloaded parking violations data to an S3 bucket for the year {year}.
    """
)

# Define task dependencies
download_task >> upload_task
