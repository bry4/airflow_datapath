import os
import pytz
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.models.param import Param

# Obtener la zona horaria deseada
TZ = os.getenv('TZ')
#timezone = pytz.timezone(TZ)

default_args = {
    'owner': 'datapath',
    # 'retries': 5,
    # 'retry_delay': timedelta(minutes=5)
}

def get_name(ti, **context):

    dag_params = context["params"]
    first_name = dag_params.get('first_name', 'Rick')
    last_name = dag_params.get('last_name', 'Hunter')

    ti.xcom_push(key='first_name', value=first_name)
    ti.xcom_push(key='last_name', value=last_name)

def get_age(ti, **context):

    dag_params = context["params"]
    age = dag_params.get('age', 18)

    var1 = Variable.get("AIRFLOW_VAR_1")
    var2_json = Variable.get("AIRFLOW_VAR_2", deserialize_json=True)
    var2 = var2_json.get('data').get('string')
    
    ti.xcom_push(key='var1', value=var1)
    ti.xcom_push(key='var2', value=var2)
    ti.xcom_push(key='age', value=age)

def greet(some_dict, ti):
    print("some dict: ", some_dict)
    first_name = ti.xcom_pull(task_ids='get_name', key='first_name')
    last_name = ti.xcom_pull(task_ids='get_name', key='last_name')
    age = ti.xcom_pull(task_ids='get_age', key='age')
    print(f"Hello World! My name is {first_name} {last_name}, "
          f"and I am {age} years old!")
    

with DAG(
    default_args=default_args,
    dag_id='03_python_operator_xcom',
    description='Python Operator - Dag Params',
    start_date=datetime(2023, 6, 28), #, tzinfo=timezone
    schedule_interval='@once',
    params={
        "first_name": Param("", type="string"),
        "last_name": Param("", type="string"),
        "age": Param(10, type="integer", minimum=10, maximum=100)
    }

) as dag:
    
    taskA = PythonOperator(
        task_id='get_name',
        python_callable=get_name,
        provide_context=True,
    )

    taskB = PythonOperator(
        task_id='get_age',
        python_callable=get_age,
        provide_context=True,
    )

    taskC = PythonOperator(
        task_id='greet',
        python_callable=greet,
        op_kwargs={'some_dict': {'a': 1, 'b': 2}}
    )

    [taskA, taskB] >> taskC