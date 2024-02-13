from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator, BigQueryCreateEmptyDatasetOperator, BigQueryCreateEmptyTableOperator
from airflow.operators.email_operator import EmailOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from datetime import timedelta

default_args = {
    'owner': 'Bryan',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['bvargasc@uni.pe'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'Example',
    default_args=default_args,
    description='descripcion',
    schedule_interval=None, #'@daily',
)

create_dataset_raw = BigQueryCreateEmptyDatasetOperator(
    task_id="create_dataset_raw",
    dataset_id="01_airflow_raw",
    gcp_conn_id="google_cloud_default",
    dag=dag,
)

create_table_raw = BigQueryCreateEmptyTableOperator(
    task_id="create_table_raw",
    dataset_id="01_airflow_raw",
    table_id="users",
    schema_fields=[
        {"name": "name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "username", "type": "STRING", "mode": "NULLABLE"},
        {"name": "email", "type": "STRING", "mode": "NULLABLE"},
    ],
    gcp_conn_id="google_cloud_default",
    dag=dag,
)

export_postgres_to_gcs = PostgresToGCSOperator(
    task_id='export_postgres_to_gcs',
    sql='SELECT * FROM users;',
    bucket='01_datapath_raw',
    filename='data/tabla_postgres.csv',
    export_format='csv',
    postgres_conn_id='postgres_datapath',
    gcp_conn_id="google_cloud_default",
    dag=dag,
)

load_gcs_to_bigquery = GCSToBigQueryOperator(
    task_id='load_gcs_to_bigquery',
    bucket='01_datapath_raw',
    source_objects=['data/tabla_postgres.csv'],
    destination_project_dataset_table='datapath-413822.01_airflow_raw.users',
    schema_fields=[
        {"name": "name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "username", "type": "STRING", "mode": "NULLABLE"},
        {"name": "email", "type": "STRING", "mode": "NULLABLE"},
    ],
    gcp_conn_id="google_cloud_default",
    write_disposition='WRITE_TRUNCATE',
    dag=dag,
)

create_dataset_model = BigQueryCreateEmptyDatasetOperator(
    task_id="create_dataset_model",
    dataset_id="02_airflow_model",
    gcp_conn_id="google_cloud_default",
    dag=dag,
)

create_table_model = BigQueryCreateEmptyTableOperator(
    task_id="create_table_model",
    dataset_id="02_airflow_model",
    table_id="users",
    schema_fields=[
        {"name": "name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "username", "type": "STRING", "mode": "NULLABLE"},
        {"name": "email", "type": "STRING", "mode": "NULLABLE"},
    ],
    gcp_conn_id="google_cloud_default",
    dag=dag,
)

transform_data_in_bigquery = BigQueryExecuteQueryOperator(
    task_id='transform_data_in_bigquery',
    sql="""
        INSERT INTO `datapath-413822.02_airflow_model.users`
        SELECT * FROM `datapath-413822.01_airflow_raw.users`;
    """,
    gcp_conn_id="google_cloud_default",
    use_legacy_sql=False,
    dag=dag,
)

def fetch_data_from_bigquery_and_prepare_email(**kwargs):
    hook = BigQueryHook(bigquery_conn_id="google_cloud_default", use_legacy_sql=False, location='US')
    sql = """SELECT name FROM `datapath-413822.02_airflow_model.users`;"""
    records = hook.get_records(sql)
    email_body = "Los nombres de usuarios son:\n\n"
    for record in records:
        email_body += f"{record}\n"
    return email_body

prepare_email_content = PythonOperator(
    task_id='prepare_email_content',
    python_callable=fetch_data_from_bigquery_and_prepare_email,
    dag=dag,
)

send_email = EmailOperator(
    task_id='send_email',
    to='bvargasc@uni.pe',
    subject='BigQuery Data Extraction',
    html_content="{{ task_instance.xcom_pull(task_ids='prepare_email') }}",
    dag=dag,
)

create_dataset_raw >> create_table_raw >> create_dataset_model >> create_table_model >> export_postgres_to_gcs >> load_gcs_to_bigquery >> transform_data_in_bigquery >> prepare_email_content >> send_email
