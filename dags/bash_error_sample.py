from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id='bash_error_sample',
    description='A simple http DAG',
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:
    bash_task = BashOperator(
        task_id='base_task',
        bash_command='date',
        # skip_exit_code=99,
    )

    error_task = BashOperator(
        task_id='error',
        bash_command='error'
    )

    bash_task >> error_task
