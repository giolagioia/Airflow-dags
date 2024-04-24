from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator

def calculateDateTime(**kwargs):
    kwargs['ti'].xcom_push(key='Date', value=f'{datetime.now().day}')
    kwargs['ti'].xcom_push(key='Minute', value=f'{datetime.now().minute}')


def dediceWhichTask(**kwargs):
    timestampMinute= int(kwargs['ti'].xcom_pull(key='Minute'))
    #print("minute: ", timestampMinute)
    if timestampMinute % 2 == 0:
        return "task2"
    else:
        return "task3"

def addDay(**kwargs):
    timestampDate= kwargs['ti'].xcom_pull(key='Date')
    newdate= int(timestampDate)+1
    print("New date is: ",newdate)

args = {
    'owner': 'airflow',
}

with DAG(
    dag_id='datetime_dag',
    start_date=datetime(2024,4,17,0),
    catchup=False

) as dag:
    task0 = PythonOperator(
        task_id='task0',
        python_callable=calculateDateTime
    )

    task1 = BranchPythonOperator(
        task_id='task1',
        python_callable=dediceWhichTask
    )

    task2 = DummyOperator(task_id='task2')

    task3 = PythonOperator(
        task_id='task3',
        python_callable=addDay
    )

task0 >> task1 >> [task2,task3]