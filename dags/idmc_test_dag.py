from airflow import DAG
from datetime import datetime, timedelta
from operators.informatica_login_operator import InformaticaLoginOperator
from operators.informatica_run_and_monitor import InformaticaRunAndMonitorJobOperator
from airflow.operators.dummy import DummyOperator
import requests
import os

default_args = {
    'owner': 'Cody',
    'depends_on_past': False,
    'start_date': datetime(2023, 5, 6),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('Informatica_IDMC_DAG', default_args=default_args, catchup=False, schedule_interval=timedelta(days=1),
          tags=["Informatica", "Changed Tag"])

begin = DummyOperator(
    task_id = "begin",
    dag=dag
)

IDMC_username = os.getenv('USERNAME')
IDMC_password = os.getenv('PASSWORD')

# Initialize Operators with necessary arguments
login_task = InformaticaLoginOperator(
    task_id="login_task",
    username=IDMC_username,
    password=IDMC_password,
)

informatica_task_id = os.getenv('informatica_task_id')
informatica_task_type= os.getenv('informatica_task_type')

run_and_monitor_task = InformaticaRunAndMonitorJobOperator(
    task_id="run_and_monitor_task",
    informatica_task_id="0022GD0Z0000000003NO",
    informatica_task_type="MTT",
    poll_interval=30  # Poll every 30 seconds
)

end = DummyOperator(
    task_id = "end",
    dag=dag
)

begin >> login_task >> run_and_monitor_task >> end
