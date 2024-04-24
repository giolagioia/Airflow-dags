from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator

args = {
    'owner': 'airflow',
}

def random_choice(bash_command, **kwargs):
    print('Date: {} and hour: {}'.format(kwargs['execution_date'], kwargs['execution_date'].minute))
    print(bash_command)
    if kwargs['execution_date'].minute % 2 == 0:
        return ["python_task1"]
    else:
        return ["python_task2"]



with DAG(
    dag_id='third_dag_example',
    default_args=args,
    start_date=datetime(2024,3,20,0),
    schedule_interval='@daily',

) as dag:

    ConditionTask=BranchPythonOperator(
        task_id='ConditionTask',
        op_kwargs= {"bash_command":'echo "Condizione di partenza"'},
        python_callable=random_choice,
)

    Task1=BashOperator(
        task_id='python_task1',
        bash_command='echo "Scelto task1"',
    )
    Task2=BashOperator(
        task_id='python_task2',
        bash_command='echo "Scelto task1"',
    )

    ConditionTask >> Task1
    ConditionTask >> Task2