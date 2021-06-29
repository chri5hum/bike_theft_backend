# from datetime import timedelta
import os
from airflow.models import DAG
from airflow.operators.python import task
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from api.bikewise_db_init import args_test
from datetime import datetime, date, timedelta
args = {
 
    'owner': 'pipis',
    'start_date': days_ago(10),
}
 
dag = DAG(dag_id = 'my_sample_dag', default_args=args, schedule_interval=timedelta(days=1))
 
 
def run_this_func():
    print('am doing time trial')
 
def run_also_this_func():
    print('I am coming last')

def print_env_var():
    print(os.environ["TEST_ENV_VAR"])
    print(os.environ["AIRFLOW__CORE__SQL_ALCHEMY_CONN"])
    print(os.environ["AIRFLOW_CTX_EXECUTION_DATE"])

# def args_test(db_string, back_delta):
#     print(db_string)
#     print(os.environ["AIRFLOW_CTX_EXECUTION_DATE"])
#     print(back_delta)

def time_trial():
    run_date_string = str(os.environ["AIRFLOW_CTX_EXECUTION_DATE"]).split("+")
    run_date_string[1] =  run_date_string[1].replace(":", "")
    run_date_string = "+".join(run_date_string)
    print("the string||" + run_date_string + "||eos")
    # run_date_string = '2021-06-28T20:23:06.631210+00:00'
    print("the string||" + run_date_string + "||eos")
    run_date_string.split("+")
    if "." in run_date_string:
        d = datetime.strptime(run_date_string, "%Y-%m-%dT%H:%M:%S.%f%z")
    else:
        d = datetime.strptime(run_date_string, "%Y-%m-%dT%H:%M:%S%z")
    print(d)
 
with dag:
    run_this_task = PythonOperator(
        task_id='run_this_first',
        python_callable = run_this_func
    )
 
    run_this_task_too = PythonOperator(
        task_id='run_this_last',
        python_callable = time_trial
    )

    env_var_task = PythonOperator(
        task_id='print_an_env_var',
        python_callable=print_env_var
    )

    test_pass_args = PythonOperator(
        task_id='args_test',
        python_callable=args_test,
        op_kwargs={'db_string': os.environ["AIRFLOW__CORE__SQL_ALCHEMY_CONN"], 'back_delta': 1},
    )
 
    run_this_task >> run_this_task_too