from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.email import send_email
from datetime import timedelta

# Email alert functions
def send_alert_email(task_id, task_status, execution_date, log_url, execution_time, task_desc, to_email):
    subject = f'Airflow Task {task_id} {task_status}'
    body = f"""
    <html> 
    <head></head>
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
    <p>SLA Miss for DAG: {dag.dag_id}</p>
    <p>Tasks that missed their SLA: {', '.join(task.task_id for task in task_list)}</p>
    <p>Blocking tasks: {', '.join(task.task_id for task in blocking_task_list)}</p>
    """
    print(f"SLA Miss detected for DAG {dag.dag_id}. Sending email...")
    send_email(to=['badreeshshetty@gmail.com'], subject=subject, html_content=html_content)
    print("SLA Miss email sent.")

# Default arguments for DAGs
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'sla': timedelta(minutes=6),  # DAG-level SLA
}

dag_dbt_run_test = DAG(
    'dbt_run_test_dag',
    default_args=default_args,
    description='DBT commands to run and test the parking violations data',
    schedule_interval=None,
    start_date=days_ago(1),
    tags=['dbt'],
)

dbt_deps = BashOperator(
    task_id='dbt_deps',
    bash_command='dbt deps --profiles-dir /opt/dbt --project-dir /opt/dbt',
    dag=dag_dbt_run_test,
    on_success_callback=success_email,
    on_failure_callback=failure_email,
)

dbt_compile = BashOperator(
    task_id='dbt_compile',
    bash_command='dbt compile --profiles-dir /opt/dbt --project-dir /opt/dbt',
    dag=dag_dbt_run_test,
    on_success_callback=success_email,
    on_failure_callback=failure_email,
)

dbt_run = BashOperator(
    task_id='dbt_run',
    bash_command='dbt run --profiles-dir /opt/dbt --project-dir /opt/dbt',
    dag=dag_dbt_run_test,
    on_success_callback=success_email,
    on_failure_callback=failure_email,
)

dbt_test = BashOperator(
    task_id='dbt_test',
    bash_command='dbt test --profiles-dir /opt/dbt --project-dir /opt/dbt',
    dag=dag_dbt_run_test,
    on_success_callback=success_email,
    on_failure_callback=failure_email,
)

trigger_dbt_docs = TriggerDagRunOperator(
    task_id='trigger_dbt_docs',
    trigger_dag_id='dbt_docs_dag',
    dag=dag_dbt_run_test,
    on_success_callback=success_email,
    on_failure_callback=failure_email,
)

# Setting task dependencies within the dbt_run_test DAG
dbt_deps >> dbt_compile >> dbt_run >> dbt_test >> trigger_dbt_docs

# DAG for dbt docs generate
dag_dbt_docs = DAG(
    'dbt_docs_dag',
    default_args=default_args,
    description='A DAG to run DBT docs generate',
    schedule_interval=None,
    start_date=days_ago(1),
    tags=['dbt'],
)

# Tasks for dbt docs generate
dbt_docs_generate = BashOperator(
    task_id='dbt_docs_generate',
    bash_command='dbt docs generate --profiles-dir /opt/dbt --project-dir /opt/dbt',
    dag=dag_dbt_docs,
    on_success_callback=success_email,
    on_failure_callback=failure_email,
)

# Setting task dependencies within the dbt_docs DAG
dbt_docs_generate
