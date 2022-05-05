import json
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator


def handle_response(response, **context):
    print(response)
    print(response.__dict__)
    print(response.content)
    response_json_as_dict = json.loads(response.content)
    print(response_json_as_dict)
    if str(response.status_code).startswith('2'):  # to catch 2XX http status code
        context['task_instance'].xcom_push(key='base_task_xcom', value='success')  # 이건 안됨이 아니라 잘됨.
        context['task_instance'].xcom_push(key='second_task_number', value=response_json_as_dict.get('next_task_number', 1))
        return True
    else:
        context['task_instance'].xcom_push(key='base_task_xcom', value='fail')  # 애초에 다음으로 진행이 안되니 무의미
        return False


def treat_as_branch(**context):
    print("Here is treat_as_branch")
    print(context)
    base_task_result = context['task_instance'].xcom_pull(key='base_task_xcom')
    next_task_number = context['task_instance'].xcom_pull(key='second_task_number')
    print("This is base_task_result")
    print(base_task_result)
    return 'http_dummy_task' + str(next_task_number)


def complete(**context):
    print(context)


with DAG(
    'http_xcom_sample',
    description='A simple http DAG',
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:
    base_task = SimpleHttpOperator(
        task_id='base_task',
        method='GET',
        endpoint='/airflow/base-task',
        http_conn_id='localhost',
        response_check=handle_response,
    )

    branch_task = BranchPythonOperator(
        task_id='branch_task',
        python_callable=treat_as_branch
    )

    http_dummy_task1 = SimpleHttpOperator(
        task_id='http_dummy_task1',
        method='GET',
        endpoint='/airflow/dummy-task1',
        http_conn_id='localhost',
    )

    http_dummy_task2 = SimpleHttpOperator(
        task_id='http_dummy_task2',
        method='GET',
        endpoint='/airflow/dummy-task2',
        http_conn_id='localhost',
    )

    http_dummy_task3 = SimpleHttpOperator(
        task_id='http_dummy_task3',
        method='GET',
        endpoint='/airflow/dummy-task3',
        http_conn_id='localhost',
    )

    complete_task = PythonOperator(
        task_id='complete_task',
        python_callable=complete
    )

    base_task >> branch_task >> [http_dummy_task1, http_dummy_task2, http_dummy_task3] >> complete_task
