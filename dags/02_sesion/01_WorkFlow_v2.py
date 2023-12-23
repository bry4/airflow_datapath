import requests
import json
from datetime import timedelta
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'bryan',
    'depends_on_past': False,
    'email': ['bryan@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='04_workflow_v2',
    default_args=default_args,
    description='A DAG that get data from a public API and send to Database',
    schedule_interval=None,
    start_date=days_ago(2),
    tags=['4rd_dag']
)
def taskflow_api_etl():

    def transform(user):
        keys = ["id", "name", "username", "email"]
        new_dict = {key: user[key] for key in keys if key in user}
        return new_dict

    @task
    def get_api_data(**kwargs):
        response = requests.get("https://jsonplaceholder.typicode.com/users")
        data = response.json()
        for user in data:
            if user["id"] == 4:
                data_dict = transform(user)
                kwargs['ti'].xcom_push(key='data_from_api', value=data_dict)

    @task
    def save_to_postgres(**kwargs):
        ti = kwargs['ti']
        data = ti.xcom_pull(key='data_from_api')
        postgres_hook = PostgresHook(postgres_conn_id='postgres_datapath')
        name = data["name"]
        username = data["username"]
        email = data["email"]
        postgres_hook.run(f"INSERT INTO users (name, username, email) VALUES ('{name}', '{username}', '{email}');")

    start_task = EmptyOperator(task_id='start')
    
    # Define the task dependencies
    get_data = get_api_data()
    save_data = save_to_postgres()

    end_task = EmptyOperator(task_id='end', trigger_rule='one_success')
    
    start_task >> get_data >> save_data >> end_task

# Instantiate the DAG
tutorial_dag = taskflow_api_etl()
