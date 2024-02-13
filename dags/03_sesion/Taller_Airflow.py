from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator, BigQueryCreateEmptyDatasetOperator, BigQueryCreateEmptyTableOperator
from airflow.operators.email_operator import EmailOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.utils.dates import days_ago
from datetime import timedelta


default_args = {
    'owner': 'Bryan',
    'email': ['bvargasc@uni.pe'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}


with DAG(
    dag_id="Taller_Airflow",
    default_args=default_args,
    description="Procesando información de Postgres a Bigquery",
    schedule_interval=None,
    start_date=days_ago(1),
) as dag:
    
    create_dataset_raw = BigQueryCreateEmptyDatasetOperator(
        task_id="create_dataset_raw",
        dataset_id="01_airflow_raw",
        gcp_conn_id="google_cloud_default"
        )
    
    create_dataset_model = BigQueryCreateEmptyDatasetOperator(
        task_id="create_dataset_model",
        dataset_id="02_airflow_model",
        gcp_conn_id="google_cloud_default"
        )

    create_dataset_analytics = BigQueryCreateEmptyDatasetOperator(
        task_id="create_dataset_analytics",
        dataset_id="03_airflow_analytics",
        gcp_conn_id="google_cloud_default"
        )

    create_table_raw_cliente = BigQueryCreateEmptyTableOperator(
        task_id = "create_table_raw_cliente",
        dataset_id = "01_airflow_raw",
        table_id = "cliente",
        schema_fields = [
            {"name":"mes", "type":"INT64", "mode":"NULLABLE"},
            {"name":"codigocliente", "type":"INT64", "mode":"NULLABLE"},
            {"name":"edad", "type":"INT64", "mode":"NULLABLE"},
            {"name":"departamento", "type":"STRING", "mode":"NULLABLE"},
            {"name":"flg_celular", "type":"INT64", "mode":"NULLABLE"},
            {"name":"flg_email", "type":"INT64", "mode":"NULLABLE"}
        ],
        gcp_conn_id = "google_cloud_default"
    )

    create_table_raw_trx = BigQueryCreateEmptyTableOperator(
        task_id = "create_table_raw_trx",
        dataset_id = "01_airflow_raw",
        table_id = "transacciones",
        schema_fields = [
            {"name":"mes", "type":"INT64", "mode":"NULLABLE"},
            {"name":"codigocliente", "type":"INT64", "mode":"NULLABLE"},
            {"name":"monto_trx", "type":"FLOAT64", "mode":"NULLABLE"},
            {"name":"cant_trx", "type":"INT64", "mode":"NULLABLE"}
        ],
        gcp_conn_id = "google_cloud_default"
    )

    postres_to_gcs_cliente = PostgresToGCSOperator(
        task_id = 'postres_to_gcs_cliente',
        sql = 'select "MES" as mes,"CODIGOCLIENTE" as codigocliente,"EDAD" as edad,"DEPARTAMENTO" as departamento,"FLG_CELULAR" as flg_celular,"FLG_EMAIL" as flg_email from clientes;',
        bucket = '01_datapath_raw',
        filename = 'data/postgres_cliente.csv',
        export_format = 'csv',
        postgres_conn_id = 'postgres_datapath',
        gcp_conn_id = "google_cloud_default"
    )

    postres_to_gcs_trx = PostgresToGCSOperator(
        task_id = 'postres_to_gcs_trx',
        sql = 'select "MES" as mes, "CODIGOCLIENTE" as codigocliente, "MONTO_TRX" as monto_trx, "CANT_TRX" as cant_trx from transacciones;',
        bucket = '01_datapath_raw',
        filename = 'data/postgres_transacciones.csv',
        export_format = 'csv',
        postgres_conn_id = 'postgres_datapath',
        gcp_conn_id = "google_cloud_default"
    )

    gcs_to_bigquery_cliente = GCSToBigQueryOperator(
        task_id = 'gcs_to_bigquery_cliente',
        bucket = '01_datapath_raw',
        source_objects = ['data/postgres_cliente.csv'],
        destination_project_dataset_table = 'datapath-413822.01_airflow_raw.cliente',
        schema_fields = [
            {"name":"mes", "type":"INT64", "mode":"NULLABLE"},
            {"name":"codigocliente", "type":"INT64", "mode":"NULLABLE"},
            {"name":"edad", "type":"INT64", "mode":"NULLABLE"},
            {"name":"departamento", "type":"STRING", "mode":"NULLABLE"},
            {"name":"flg_celular", "type":"INT64", "mode":"NULLABLE"},
            {"name":"flg_email", "type":"INT64", "mode":"NULLABLE"}
        ],
        gcp_conn_id = "google_cloud_default",
        write_disposition = "WRITE_TRUNCATE"
    )

    gcs_to_bigquery_trx = GCSToBigQueryOperator(
        task_id = 'gcs_to_bigquery_trx',
        bucket = '01_datapath_raw',
        source_objects = ['data/postgres_transacciones.csv'],
        destination_project_dataset_table = 'datapath-413822.01_airflow_raw.transacciones',
        schema_fields = [
            {"name":"mes", "type":"INT64", "mode":"NULLABLE"},
            {"name":"codigocliente", "type":"INT64", "mode":"NULLABLE"},
            {"name":"monto_trx", "type":"FLOAT64", "mode":"NULLABLE"},
            {"name":"cant_trx", "type":"INT64", "mode":"NULLABLE"}
        ],
        gcp_conn_id = "google_cloud_default",
        write_disposition = "WRITE_TRUNCATE"
    )

    create_table_modelo = BigQueryCreateEmptyTableOperator(
        task_id = "create_table_modelo",
        dataset_id = "02_airflow_model",
        table_id = "resultado",
        schema_fields = [
            {"name":"mes", "type":"INT64", "mode":"NULLABLE"},
            {"name":"codigocliente", "type":"INT64", "mode":"NULLABLE"},
            {"name":"edad", "type":"INT64", "mode":"NULLABLE"},
            {"name":"monto_trx", "type":"FLOAT64", "mode":"NULLABLE"}
        ],
        gcp_conn_id = "google_cloud_default"
    )

    etl_modelo = BigQueryExecuteQueryOperator(
        task_id = "etl_modelo",
        sql = """
            INSERT INTO `datapath-413822.02_airflow_model.resultado`
            select A.mes as mes,A.codigocliente as codigocliente,A.edad as edad,B.monto_trx as monto_trx from
            (select * from `datapath-413822.01_airflow_raw.cliente` 
            where flg_celular = 1
            and flg_email = 1
            and departamento = 'LIMA'
            and edad >30) A
            inner join
            (select * from `datapath-413822.01_airflow_raw.transacciones`
            where cant_trx > 1) B
            on A.codigocliente = B.codigocliente
            and A.mes = B.mes;
        """
    )

    def fetch_data_from_bigquery_and_prepare_email(**kwargs):
        hook = BigQueryHook(bigquery_conn_id="google_cloud_default", use_legacy_sql=False, location='US')
        sql = """select * from `datapath-413822.02_airflow_model.resultado` WHERE codigocliente=29111047"""
        records = hook.get_records(sql)
        email_body = "Los datos del cliente son:\n\n"
        for record in records:
            email_body += f"{record}\n"
        return email_body

    prepare_email = PythonOperator(
        task_id = 'prepare_email',
        python_callable = fetch_data_from_bigquery_and_prepare_email
    )


    send_email = EmailOperator(
        task_id='send_email',
        to = 'bvargasc@uni.pe',
        subject = 'Taller Airflow Sesion 3',
        html_content = "{{ task_instance.xcom_pull(task_ids='prepare_email') }}"
    )


### ORQUESTACION DE TAREAS

    create_dataset_raw >> [create_table_raw_cliente, create_table_raw_trx]

    create_table_raw_cliente >> postres_to_gcs_cliente >> gcs_to_bigquery_cliente

    create_table_raw_trx >> postres_to_gcs_trx >> gcs_to_bigquery_trx

    [gcs_to_bigquery_cliente,gcs_to_bigquery_trx] >> create_dataset_model >> create_dataset_analytics

    create_dataset_model >> create_table_modelo >> etl_modelo >> prepare_email >> send_email