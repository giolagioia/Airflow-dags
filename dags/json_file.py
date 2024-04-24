from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
import json


'''
#with open('C:/Users/g.lagioia/docker/airflow/dags/test_config.json', 'r') as file:
    json_data = json.load(file)
'''

json_data={
    "step1":
        {"command": "echo 'step1'"},
    "step2":
        {"command": "echo 'step2'"},
    "step3":
        {"command": "echo 'step3'"},
}


def loadtoXCOM(command):
    for key, value in command.items():
        return value

def decideWhichTask(**kwargs):
    stepvalue = kwargs['ti'].xcom_pull(key='return_value')
    stepint = stepvalue.split("step")
    stepint = int(stepint[1])

    if stepint % 2 == 0:
        return stepvalue+"_dummy_pari"
    else:
        return stepvalue+"_dummy_dispari"


with DAG(
    dag_id='json_file',
    start_date=datetime(2024,4,17,0),
    catchup=False,

) as dag:
    for step,command in json_data.items():
        jsonRead = PythonOperator(
            task_id=step+'_JsonRead',
            python_callable=loadtoXCOM,
            op_kwargs={"command": command},
        )

        BashCommand= BashOperator(
            task_id=step+'_BashCommand',
            bash_command = f"{{{{ ti.xcom_pull(key='return_value', task_ids='{step + '_JsonRead'}') }}}}"
            #bash_command="{{ ti.xcom_pull(key='command') }}"
        )

        Decidewhichtask = BranchPythonOperator(
            task_id=step+'_Decidewhichtask',
            python_callable=decideWhichTask,
        )

        Dummy_pari = DummyOperator(
            task_id=step+'_dummy_pari'
        )

        Dummy_dispari = DummyOperator(
            task_id=step+'_dummy_dispari'
        )

        '''
        BranchPO che estrae da xcom del bash operator (da returned value) x di "echo x",
        if x%2 == 0  -> dummy operator "step_x pari"
        else  -> dummy operator "step_x dispari"
        '''

        jsonRead >> BashCommand >> Decidewhichtask >> [Dummy_pari, Dummy_dispari]

