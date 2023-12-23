from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from datetime import timedelta, datetime

default_args = {
    'owner': 'bryan',
}

with DAG(
    '01_first_dag',
    default_args=default_args,
    description='Create first DAG',
    schedule_interval=None, #timedelta(minutes = 5),
    start_date=days_ago(1), #datetime(2023,5,13,17,25),
    tags=['1st_dag'],
) as dag:

    start_task = EmptyOperator(
        task_id='start'
    )
    task1 = DummyOperator(
        task_id='task_1',
    )
    task2 = DummyOperator(
        task_id='task_2',
    )

    end_task = EmptyOperator(
        task_id='end'
    )

    start_task >> task1 >> task2 >> end_task
