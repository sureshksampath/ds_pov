from airflow import DAG
from airflow.utils.dates import days_ago
from pendulum import duration
from informatica_run_and_monitor_operator import InformaticaRunAndMonitorJobOperator  # Import your custom operator

# Define default_args with retry logic
default_args = {
    'retries': 3,  # Number of retries
    'retry_delay': duration(seconds=30),  # Time to wait between retries
    'retry_exponential_backoff': True,  # Use exponential backoff
    'max_retry_delay': duration(hours=2),  # Maximum retry delay
}

# Define the DAG
with DAG(
    dag_id='informatica_job_retry_dag',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="@daily",
    catchup=False
) as dag:

    # Create a task using your custom operator
    informatica_job = InformaticaRunAndMonitorJobOperator(
        task_id="run_informatica_job",
        informatica_task_id="some_task_id",
        informatica_task_type="some_task_type",
        poll_interval=30,  # Polling interval in seconds
        retries=5,  # Override the default 3 retries with 5 for this specific task
        retry_delay=duration(minutes=1),  # Override the retry delay
    )
