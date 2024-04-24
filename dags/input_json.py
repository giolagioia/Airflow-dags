import os
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator

def print_input():
    print("Text provided by input in 'args': {}".format(os.environ.get('args'))),
    print("Test option by input in 'option': {}".format(os.environ.get('option')))

def coiche_task():
    if os.environ.get('option'):
        return "option_true"
    else:
        return "option_false"

def unique_function():
    print_input(),
    return coiche_task()

with DAG(
    dag_id='input_json',
    start_date=datetime(2024,4,12,0),
    schedule_interval='@daily',
    catchup=False,
    default_args={
        'owner': 'airflow'
    }
) as dag:
    get_info=BranchPythonOperator(
        task_id='get_info',
        python_callable=unique_function,
    )

    option_true=BashOperator(
        task_id='option_true',
        bash_command='echo "True selected correctly"'
    )

    option_false=BashOperator(
        task_id='option_false',
        bash_command='echo "False selected correctly"'
    )

get_info >> [option_true, option_false]
