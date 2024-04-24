import os
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator

def ReadInput(**kwargs):
    kwargs['ti'].xcom_push(key='input', value=os.environ.get("INPUT_DATA"))

def WriteInput(**kwargs):
    print("Input data: {}", format(kwargs['ti'].xcom_pull(key='input')))

with DAG(
    dag_id='input_xcom',
    start_date=datetime(2024,4,10,0),
    schedule_interval='@weekly',
    catchup=False,

) as dag:
    readInput = PythonOperator(
        task_id='ReadInput',
        python_callable=ReadInput
    )

    printOutput = PythonOperator(
        task_id='printOutput',
        python_callable=WriteInput
    )

readInput >> printOutput



