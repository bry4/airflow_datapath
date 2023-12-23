import requests
import json
from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago


def get_api_data(**kwargs):
    response = requests.get("https://jsonplaceholder.typicode.com/users")
    data = response.json()
    for user in data:
        if user["id"] == 4:
            data_dict = transform(user)
            kwargs['ti'].xcom_push(key='data_from_api', value=data_dict)
            #df = pd.DataFrame(data_dict)
            #df.to_csv('/tmp/user.csv', sep='\t', index=False, header=False)

def transform(user):
    keys = ["id","name", "username","email"]
    new_dict = {key: user[key] for key in keys if key in user}
    return new_dict

def save_to_postgres(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(key='data_from_api')
    postgres_hook = PostgresHook(postgres_conn_id='postgres_datapath')
    name = data["name"]
    username = data["username"]
    email = data["email"]
    postgres_hook.run(f"INSERT INTO users (name, username, email) VALUES ('{name}', '{username}', '{email}');")


default_args = {
    'owner': 'bryan',
    'depends_on_past': False,
    'email': ['bryan@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    '04_workflow_v1',
    default_args=default_args,
    description='A DAG that get data from a public API and send to Database',
    schedule_interval=None,
    start_date=days_ago(2),
    tags=['3rd_dag']
)

start_task = EmptyOperator(
    task_id='start',
    dag=dag,
)

get_data = PythonOperator(
    task_id='get_data_from_api',
    python_callable=get_api_data,
    provide_context=True,
)
    
save_data = PythonOperator(
    task_id='save_data_to_postgres',
    python_callable=save_to_postgres,
    provide_context=True,
)

end_task = EmptyOperator(
    task_id='end',
    trigger_rule='one_success',
    dag=dag,
)

start_task >> get_data >> save_data >> end_task

