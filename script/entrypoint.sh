#!/bin/bash
set -e

if [ -e "/opt/airflow/requirements.txt" ]; then
    $(command -v pip) install --user -r requirements.txt
fi

# Initialize the database if it hasn't been initialized yet
if [ ! -f "/opt/airflow/airflow.db" ]; then
  airflow db init && \
  airflow users create \
    --username airflow \
    --firstname airflow \
    --lastname airflow \
    --role Admin \
    --email airflow@gmail.com \
    --password airflow!
fi

$(command -v airflow) db migrate

exec airflow webserver