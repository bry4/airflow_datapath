from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import datetime

def branch_function():
    # Implement business logic here
    current_date = datetime.now()
    day_of_week = current_date.weekday()
    days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    dow = days[day_of_week]

    if dow in ["Sunday"]:
        return 'python_task'
    else:
        return 'bash_task'

# Function executed for PythonOperator
def hello_function():
    print("It's Weekend, Hello World from PythonOperator!")

# These are the default arguments that will be supplied to the operators
default_args = {
    'owner': 'bryan',
    'depends_on_past': False,
    'email': ['bryan@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}

# This is where we define our DAG
dag = DAG(
    '02_basic_operators',
    default_args=default_args,
    description='Basic Operators DAG',
    schedule_interval=None,
    start_date=days_ago(1),
    tags=['2nd_dag'],
)

start_task = EmptyOperator(
    task_id='start',
    dag=dag,
)

branching = BranchPythonOperator(
    task_id='branching',
    python_callable=branch_function,
)

pythonprint = PythonOperator(
    task_id='python_task',
    python_callable=hello_function,
    dag=dag,
)

bashprint = BashOperator(
    task_id='bash_task',
    bash_command='echo "Its time to work, Hello World from BashOperator!"',
    dag=dag,
)

end_task = EmptyOperator(
    task_id='end',
    trigger_rule='one_success',
    dag=dag,
)

start_task >> branching >> pythonprint >> end_task
start_task >> branching >> bashprint >> end_task



