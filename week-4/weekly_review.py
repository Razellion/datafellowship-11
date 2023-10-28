from airflow import DAG
from airflow.models import Variable
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator, BigQueryInsertJobOperator
from airflow.operators.python import PythonOperator,BranchPythonOperator
from datetime import datetime, timedelta
import os

default_args={
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

LOCAL_SINK_PATH = '/opt/airflow/dags/output/'
DATASET_NAME = os.environ.get("GCP_DATASET_NAME", 'w4_assignment')
RAW_USER_TABLE_NAME = os.environ.get("GCP_RAW_USER_TABLE_NAME", 'raw_users')
RAW_CART_TABLE_NAME = os.environ.get("GCP_RAW_CART_TABLE_NAME", 'raw_carts')
RAW_POST_TABLE_NAME = os.environ.get("GCP_RAW_POST_TABLE_NAME", 'raw_posts')
RAW_TODO_TABLE_NAME = os.environ.get("GCP_RAW_TODO_TABLE_NAME", 'raw_todos')
last_user_row = Variable.get('last_user_row', default_var=0)
last_cart_row = Variable.get('last_cart_row', default_var=0)
last_post_row = Variable.get('last_post_row', default_var=0)
last_todo_row = Variable.get('last_todo_row', default_var=0)

def check_if_last_row(task_now, object, next_task, **kwargs):
    import json
    ti = kwargs['ti']
    res = ti.xcom_pull(task_ids=task_now)
    data = json.loads(res)
    if len(data[object]) == 0: return
    return next_task

def store_object(task_name, object_name, object_variable_key, object_variable_value, **kwargs):
    import json
    ti = kwargs['ti']
    api_data = ti.xcom_pull(task_ids=task_name)
    data = json.loads(api_data)
    objects = data[object_name]
    last_object_id = object_variable_value
    with open(f"{LOCAL_SINK_PATH}{object_name}.json", 'w', encoding='utf8') as f:
        for obj in objects:
            f.write(f'{json.dumps(obj)}\n')
        last_object_id = obj["id"]
        Variable.set(object_variable_key,last_object_id)

with DAG(
    'weekly_review_dag',
    default_args=default_args,
    description='Dummy JSON usecase',
    schedule_interval="0 0 * * *",
    start_date=datetime(2023, 10, 27),
    tags=['iykra'],
    ) as dag:

    create_assignment_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='create_w4_assignment_dataset', dataset_id=DATASET_NAME, dag=dag, gcp_conn_id="gcp_conn"
    )

    is_user_api_available_task = HttpSensor(
        task_id="is_user_api_available_task",
        http_conn_id='dummyjson_conn',
        endpoint='/users/?limit=1',
        timeout=10,
        mode='poke',
        poke_interval=60,
        retries=5,
    )

    is_cart_api_available_task = HttpSensor(
        task_id="is_cart_api_available_task",
        http_conn_id='dummyjson_conn',
        endpoint='/carts/?limit=1',
        timeout=10,
        mode='poke',
        poke_interval=60,
        retries=5,
    ),

    is_post_api_available_task = HttpSensor(
        task_id="is_post_api_available_task",
        http_conn_id='dummyjson_conn',
        endpoint='/posts/?limit=1',
        timeout=10,
        mode='poke',
        poke_interval=60,
        retries=5,
    ),

    is_todo_api_available_task = HttpSensor(
        task_id="is_todo_api_available_task",
        http_conn_id='dummyjson_conn',
        endpoint='/todos/?limit=1',
        timeout=10,
        mode='poke',
        poke_interval=60,
        retries=5,
    ),

    extract_user_task = SimpleHttpOperator(
        task_id="extract_user_task",
        http_conn_id='dummyjson_conn',
        method='GET',
        endpoint=f'/users?skip={last_user_row}',
        headers={"Content-Type": "application/json"},
        log_response=True,
        response_check=lambda response: True if response.status_code == 200 else False
    )

    extract_cart_task = SimpleHttpOperator(
        task_id="extract_cart_task",
        http_conn_id='dummyjson_conn',
        method='GET',
        endpoint=f'/carts?skip={last_cart_row}',
        headers={"Content-Type": "application/json"},
        log_response=True,
        response_check=lambda response: True if response.status_code == 200 else False
    )

    extract_post_task = SimpleHttpOperator(
        task_id="extract_post_task",
        http_conn_id='dummyjson_conn',
        method='GET',
        endpoint=f'/posts?skip={last_post_row}',
        headers={"Content-Type": "application/json"},
        log_response=True,
        response_check=lambda response: True if response.status_code == 200 else False
    )

    extract_todo_task = SimpleHttpOperator(
        task_id="extract_todo_task",
        http_conn_id='dummyjson_conn',
        method='GET',
        endpoint=f'/todos?skip={last_todo_row}',
        headers={"Content-Type": "application/json"},
        log_response=True,
        response_check=lambda response: True if response.status_code == 200 else False
    )

    check_for_new_user_task = BranchPythonOperator(
        task_id="check_for_new_user_task",
        python_callable=check_if_last_row,
        op_kwargs={'task_now': "extract_user_task", 'object': "users", 'next_task': "store_user_task"},
    )

    check_for_new_cart_task = BranchPythonOperator(
        task_id="check_for_new_cart_task",
        python_callable=check_if_last_row,
        op_kwargs={'task_now': "extract_cart_task", 'object': "carts", 'next_task': "store_cart_task"},
    )

    check_for_new_post_task = BranchPythonOperator(
        task_id="check_for_new_post_task",
        python_callable=check_if_last_row,
        op_kwargs={'task_now': "extract_post_task", 'object': "posts", 'next_task': "store_post_task"},
    )

    check_for_new_todo_task = BranchPythonOperator(
        task_id="check_for_new_todo_task",
        python_callable=check_if_last_row,
        op_kwargs={'task_now': "extract_todo_task", 'object': "todos", 'next_task': "store_todo_task"},
    )

    store_user_task = PythonOperator(
        task_id="store_user_task",
        python_callable=store_object,
        op_kwargs={'task_name': "extract_user_task",
                   'object_name': "users",
                   'object_variable_key':"last_user_row",
                   'object_variable_value': last_user_row
                  },
    )

    store_cart_task = PythonOperator(
        task_id="store_cart_task",
        python_callable=store_object,
        op_kwargs={'task_name': "extract_cart_task",
                   'object_name': "carts",
                   'object_variable_key':"last_cart_row",
                   'object_variable_value': last_cart_row
                  },
    )

    store_post_task = PythonOperator(
        task_id="store_post_task",
        python_callable=store_object,
        op_kwargs={'task_name': "extract_post_task",
                   'object_name': "posts",
                   'object_variable_key':"last_post_row",
                   'object_variable_value': last_post_row
                  },
    )

    store_todo_task = PythonOperator(
        task_id="store_todo_task",
        python_callable=store_object,
        op_kwargs={'task_name': "extract_todo_task",
                   'object_name': "todos",
                   'object_variable_key':"last_todo_row",
                   'object_variable_value': last_todo_row
                  },
    )

    user_local_to_gcs_task = LocalFilesystemToGCSOperator(
        task_id='user_transfer_to_gcs',
        src='/opt/airflow/dags/output/users.json',
        dst=f'w4-assignment/data/users-{last_user_row}.json',
        bucket='data-fellowship-400609',
        gcp_conn_id='gcp_conn',
        dag=dag,
    )

    cart_local_to_gcs_task = LocalFilesystemToGCSOperator(
        task_id='cart_transfer_to_gcs',
        src='/opt/airflow/dags/output/carts.json',
        dst=f'w4-assignment/data/carts-{last_cart_row}.json',
        bucket='data-fellowship-400609',
        gcp_conn_id='gcp_conn',
        dag=dag,
    )

    post_local_to_gcs_task = LocalFilesystemToGCSOperator(
        task_id='post_transfer_to_gcs',
        src='/opt/airflow/dags/output/posts.json',
        dst=f'w4-assignment/data/posts-{last_post_row}.json',
        bucket='data-fellowship-400609',
        gcp_conn_id='gcp_conn',
        dag=dag,
    )

    todo_local_to_gcs_task = LocalFilesystemToGCSOperator(
        task_id='todo_transfer_to_gcs',
        src='/opt/airflow/dags/output/todos.json',
        dst=f'w4-assignment/data/todos-{last_todo_row}.json',
        bucket='data-fellowship-400609',
        gcp_conn_id='gcp_conn',
        dag=dag,
    )

    user_gcs_to_bigquery_task = GCSToBigQueryOperator(
        task_id="user_gcs_to_bigquery_task",
        bucket="data-fellowship-400609",
        source_objects=[f"w4-assignment/data/users-{last_user_row}.json"],
        source_format="NEWLINE_DELIMITED_JSON",
        destination_project_dataset_table=f"{DATASET_NAME}.{RAW_USER_TABLE_NAME}",
        write_disposition="WRITE_APPEND",
        external_table=False,
        autodetect=True,
        deferrable=True,
        gcp_conn_id="gcp_conn",
        dag=dag
    )

    cart_gcs_to_bigquery_task = GCSToBigQueryOperator(
        task_id="cart_gcs_to_bigquery_task",
        bucket="data-fellowship-400609",
        source_objects=[f"w4-assignment/data/carts-{last_cart_row}.json"],
        source_format="NEWLINE_DELIMITED_JSON",
        destination_project_dataset_table=f"{DATASET_NAME}.{RAW_CART_TABLE_NAME}",
        write_disposition="WRITE_APPEND",
        external_table=False,
        autodetect=True,
        deferrable=True,
        gcp_conn_id="gcp_conn",
        dag=dag
    )

    post_gcs_to_bigquery_task = GCSToBigQueryOperator(
        task_id="post_gcs_to_bigquery_task",
        bucket="data-fellowship-400609",
        source_objects=[f"w4-assignment/data/posts-{last_post_row}.json"],
        source_format="NEWLINE_DELIMITED_JSON",
        destination_project_dataset_table=f"{DATASET_NAME}.{RAW_POST_TABLE_NAME}",
        write_disposition="WRITE_APPEND",
        external_table=False,
        autodetect=True,
        deferrable=True,
        gcp_conn_id="gcp_conn",
        dag=dag
    )

    todo_gcs_to_bigquery_task = GCSToBigQueryOperator(
        task_id="todo_gcs_to_bigquery_task",
        bucket="data-fellowship-400609",
        source_objects=[f"w4-assignment/data/todos-{last_todo_row}.json"],
        source_format="NEWLINE_DELIMITED_JSON",
        destination_project_dataset_table=f"{DATASET_NAME}.{RAW_TODO_TABLE_NAME}",
        write_disposition="WRITE_APPEND",
        external_table=False,
        autodetect=True,
        deferrable=True,
        gcp_conn_id="gcp_conn",
        dag=dag
    )

    user_cart_summary_query_job = BigQueryInsertJobOperator(
        task_id="user_cart_summary_query_job",
        configuration={
            "query": {
                "query":'CREATE OR REPLACE TABLE data-fellowship-400609.w4_assignment.user_cart_summary as '
                        'SELECT u.id, firstName, lastName, totalProducts, totalQuantity, total FROM `data-fellowship-400609.w4_assignment.raw_carts` c '
                        'JOIN `data-fellowship-400609.w4_assignment.raw_users` u ON c.userId = u.id '
                        'ORDER BY total',
                "useLegacySql": False,
                "priority": "BATCH",
            }
        },
        location="US",
        gcp_conn_id="gcp_conn",
        dag=dag
    )

    product_cart_summary_query_job = BigQueryInsertJobOperator(
        task_id="product_cart_summary_query_job",
        configuration={
            "query": {
                "query":'CREATE OR REPLACE TABLE data-fellowship-400609.w4_assignment.product_cart_summary as '
                        'SELECT p.title, SUM(quantity) as total_qty, SUM(price) as total_price FROM `data-fellowship-400609.w4_assignment.raw_carts`, UNNEST(products) as p '
                        'GROUP BY p.title, discountPercentage '
                        'ORDER BY total_qty DESC',
                "useLegacySql": False,
                "priority": "BATCH",
            }
        },
        location="US",
        gcp_conn_id="gcp_conn",
        dag=dag
    )


#User Task
create_assignment_dataset >> is_user_api_available_task >>\
extract_user_task >> check_for_new_user_task >> store_user_task >> user_local_to_gcs_task >>\
user_gcs_to_bigquery_task

#cart Task
create_assignment_dataset >> is_cart_api_available_task >>\
extract_cart_task >> check_for_new_cart_task >> store_cart_task >> cart_local_to_gcs_task >>\
cart_gcs_to_bigquery_task

#post Task
create_assignment_dataset >> is_post_api_available_task >>\
extract_post_task >> check_for_new_post_task >> store_post_task >> post_local_to_gcs_task >>\
post_gcs_to_bigquery_task

#todo Task
create_assignment_dataset >> is_todo_api_available_task >>\
extract_todo_task >> check_for_new_todo_task >> store_todo_task >> todo_local_to_gcs_task >>\
todo_gcs_to_bigquery_task

user_cart_summary_query_job
product_cart_summary_query_job