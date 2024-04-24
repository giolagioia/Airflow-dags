from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

args = {
    'owner': 'airflow',
}


with DAG(
    dag_id='second_dag_example',
    default_args=args,
    start_date=datetime(2024,3,20,0),
    schedule_interval='@daily',

) as dag:
    task1 = BashOperator(
        task_id='task1',
        bash_command='echo "task 1"',
    )
    task2 = BashOperator(
        task_id='task2',
        bash_command='echo "task 2"',
    )
    task3 = BashOperator(
        task_id='task3',
        bash_command='echo "task 3"',
    )

    task1 >> task2 >> task3

