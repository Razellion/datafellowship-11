import os

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

DATASET_NAME = os.environ.get("GCP_DATASET_NAME", 'raw')
TABLE_NAME = os.environ.get("GCP_TABLE_NAME", 'nyc_taxi_trip')

with DAG(
    dag_id='weekly_assignment_5',
    start_date=datetime(2023, 11, 3),
    schedule_interval=None,
    tags=['iykra'],
) as dag:

    nyc_local_to_gcs_task = LocalFilesystemToGCSOperator(
        task_id='nyc_transfer_to_gcs',
        src='/opt/airflow/dags/output/taxi_tripdata.csv',
        dst=f'w5-assignment/data/taxi_tripdata.csv',
        bucket='data-fellowship-400609',
        gcp_conn_id='gcp_conn',
        dag=dag,
    )

    usd_local_to_gcs_task = LocalFilesystemToGCSOperator(
        task_id='usd_transfer_to_gcs',
        src='/opt/airflow/dags/output/usd_idr_rate.csv',
        dst=f'w5-assignment/data/usd_idr_rate.csv',
        bucket='data-fellowship-400609',
        gcp_conn_id='gcp_conn',
        dag=dag,
    )

    create_raw_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='create_nyc_taxi_dataset', dataset_id=DATASET_NAME, dag=dag, gcp_conn_id="gcp_conn"
    )

    load_nyc_taxi_csv = GCSToBigQueryOperator(
        task_id='nyc_taxi_gcs_to_bigquery',
        bucket='data-fellowship-400609',
        source_objects=['w5-assignment/data/taxi_tripdata.csv'],
        destination_project_dataset_table=f"{DATASET_NAME}.{TABLE_NAME}",
        source_format='CSV',
        autodetect=True,
        write_disposition='WRITE_TRUNCATE',
        gcp_conn_id="gcp_conn",
        skip_leading_rows=1,
        dag=dag,
    )

    load_usd_idr_csv = GCSToBigQueryOperator(
        task_id='usd_idr_gcs_to_bigquery',
        bucket='data-fellowship-400609',
        source_objects=['w5-assignment/data/usd_idr_rate.csv'],
        destination_project_dataset_table=f"{DATASET_NAME}.usd_idr_rate",
        source_format='CSV',
        autodetect=True,
        write_disposition='WRITE_TRUNCATE',
        gcp_conn_id="gcp_conn",
        skip_leading_rows=1,
        dag=dag,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="dbt run --project-dir /opt/airflow/dags/dbts/w5_assignment --profiles-dir /opt/airflow/dags/dbts/w5_assignment",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="dbt test --project-dir /opt/airflow/dags/dbts/w5_assignment --profiles-dir /opt/airflow/dags/dbts/w5_assignment",
    )

nyc_local_to_gcs_task >> usd_local_to_gcs_task >> create_raw_dataset >> load_nyc_taxi_csv >> load_usd_idr_csv >> dbt_run >> dbt_test
