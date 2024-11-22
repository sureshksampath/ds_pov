# In your Airflow DAG file
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.ssh_operator import SSHOperator
from airflow.models import Variable
import requests
import os

from datetime import datetime, timedelta
default_args = {
'owner': 'your_name',
'start_date': datetime(2024, 1, 1),
'retries': 1,
'retry_delay': timedelta(minutes=5),
}
 
dag = DAG(
'remote_job_trigger',
default_args=default_args,
schedule='@daily',
catchup=False,
)
 
ssh_task = SSHOperator(
task_id='execute_remote_job',
command='echo Hello World',
hostname=51.8.81.136,
key_file='/path/to/private_key',
port=22,
dag=dag,
)
