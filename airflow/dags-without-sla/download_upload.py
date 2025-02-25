from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.dates import days_ago
from datetime import datetime
import os
import requests
import gzip
import shutil
import time

# Constants
S3_BUCKET = 'parking-violations-bucket'
DATA_FOLDER = 'data'

# Ensure data folder exists
os.makedirs(DATA_FOLDER, exist_ok=True)

# Define default args for DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(2),
    'retries': 1,
}

# Define the DAG
dag = DAG(
    'parking_violations_download_upload',
    default_args=default_args,
    description='Download and upload parking violations data to S3',
    schedule_interval='@once',
)

# Task to download CSV files locally
def download_csv_files():
    print("Download task started.")
    start_time = time.time()
    
    # URLs of the CSV files to download
    urls = {
        "DOF_Parking_Violation_Codes.csv": 'https://data.cityofnewyork.us/api/views/ncbg-6agr/rows.csv?accessType=DOWNLOAD',
        "Parking_Violations_Issued_Fiscal_Year_2024.csv": 'https://data.cityofnewyork.us/api/views/pvqr-7yc4/rows.csv?accessType=DOWNLOAD&api_foundry=true'
    }
    
    # Download each file
    for filename, url in urls.items():
        local_file_path = os.path.join(DATA_FOLDER, filename)
        response = requests.get(url)
        with open(local_file_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)
        print(f"Downloaded {filename} locally at {local_file_path}")
        
        # Check if the file is the large one and compress it
        if filename == "Parking_Violations_Issued_Fiscal_Year_2024.csv":
            gzip_file_path = local_file_path + '.gz'
            with open(local_file_path, 'rb') as f_in, gzip.open(gzip_file_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
            print(f"Compressed {filename} to {gzip_file_path}")
            os.remove(local_file_path)  # Remove the original large CSV file
            print(f"Removed original file {local_file_path}")
    
    end_time = time.time()
    duration = (end_time - start_time) / 60
    print(f"Download task completed in {duration:.2f} minutes")

# Create the download task in the DAG
download_task = PythonOperator(
    task_id='download_csv_files',
    python_callable=download_csv_files,
    dag=dag,
)

# Task to upload downloaded files to S3
def upload_files_to_s3():
    print("Upload task started.")
    start_time = time.time()
    
    # Create S3 hook
    s3_hook = S3Hook(aws_conn_id='aws_default')
    
    # Upload each file in the data folder to S3
    for filename in os.listdir(DATA_FOLDER):
        local_file_path = os.path.join(DATA_FOLDER, filename)
        if os.path.isfile(local_file_path):
            s3_hook.load_file(local_file_path, key=filename, bucket_name=S3_BUCKET, replace=True)
            print(f"Uploaded {filename} to S3 bucket {S3_BUCKET}")
    
    end_time = time.time()
    duration = (end_time - start_time) / 60
    print(f"Upload task completed in {duration:.2f} minutes")

# Create the upload task in the DAG
upload_task = PythonOperator(
    task_id='upload_files_to_s3',
    python_callable=upload_files_to_s3,
    dag=dag,
)

# Define task dependencies
download_task >> upload_task
