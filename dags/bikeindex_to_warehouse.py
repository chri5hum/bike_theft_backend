# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import days_ago

from api.bikewise_db_init import intoDB, intoDBToday, capTransactions
from api.bikewise_v2 import getBikeInfo
from datetime import datetime, timedelta

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}
with DAG(
    'bikewise_to_warehouse',
    default_args=default_args,
    description='DAG to grab shit from BikeIndex API to Data Warehouse',
    schedule_interval=timedelta(days=5), #means it runs every day, probably at 00:00 GMT
    start_date=days_ago(300),
    max_active_runs=1,
    tags=['example'],
) as dag:

    # t1, t2 and t3 are examples of tasks created by instantiating operators
    # t1 = BashOperator(
    #     task_id='print_date',
    #     bash_command='date',
    # )

    # t2 = BashOperator(
    #     task_id='sleep',
    #     depends_on_past=False,
    #     bash_command='sleep 5',
    #     retries=3,
    # )
    bike_db = "postgres_bikes"

    create_schemas = PostgresOperator(
        task_id='create_schemas',
        postgres_conn_id="postgres_bikes",
        sql='sql/create_schemas.sql'
    )

    create_date_dim = PostgresOperator(
        task_id='create_date_dim',
        postgres_conn_id="postgres_bikes",
        sql='sql/create_date_dim.sql'
    )

    create_tables = PostgresOperator(
        task_id='create_tables',
        postgres_conn_id="postgres_bikes",
        sql='sql/create_tables.sql'
    )

    bikewise_to_postgres = PythonOperator(
        task_id='bikewise_to_postgres',
        python_callable=intoDBToday,
        # op_args=['arguments_passed_to_callable'],
        op_kwargs={'postgres_conn_id': bike_db, 'days_back': 20, 'time_delta': 15}
    )

    staging = PostgresOperator(
        task_id='staging',
        postgres_conn_id="postgres_bikes",
        sql='sql/staging.sql'
    )

    load_fact = PostgresOperator(
        task_id='load_fact',
        postgres_conn_id="postgres_bikes",
        sql='sql/load_fact.sql'
    )

    cap_transactions = PythonOperator(
        task_id='cap_transactions',
        python_callable=capTransactions,
        op_kwargs={'postgres_conn_id': bike_db, 'days_back': 20, 'time_delta': 15}

    )

    create_schemas >> create_date_dim >> create_tables >> bikewise_to_postgres >> staging >> load_fact >> cap_transactions
