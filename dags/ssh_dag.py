from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy import DummyOperator
from airflow.operators.ssh_operator import SSHOperator


default_args = {
    'owner': 'Cody',
    'depends_on_past': False,
    'start_date': datetime(2024, 11, 11),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('ssh_dag', default_args=default_args, catchup=False, schedule_interval=None, tags=["ssh"])

begin = DummyOperator(
    task_id = "begin",
    dag=dag
)

ssh_task = SSHOperator(
task_id = 'ssh_task',
ssh_conn_id = 'ssh_conn',
command = 'bash ~/firstfile.sh',
dag = dag,
)

end = DummyOperator(
    task_id = "end",
    dag=dag
)

#begin >> login_task >> run_and_monitor_task >> run_and_monitor_task2 >> end
begin >> ssh_task >> end
