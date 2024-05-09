from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.models.baseoperator import chain
from airflow.utils.trigger_rule import TriggerRule
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

def decideWhichTask(step, **kwargs):
    task_id = f"{step}_BashCommand"
    stepvalue = kwargs['ti'].xcom_pull(key='return_value', task_ids=task_id)
    stepint = int(stepvalue[-1])

    if stepint % 2 == 0:
        return stepvalue+"_dummy_pari"
    else:
        return stepvalue+"_dummy_dispari"


with DAG(
    dag_id='json_file_chain',
    start_date=datetime(2024,4,17,0),
    catchup=False,

) as dag:
    tasklist= []
    prec_dummy_pari = None
    prec_dummy_dispari = None

    for step,command in json_data.items():
        jsonRead = PythonOperator(
            task_id=step+'_JsonRead',
            python_callable=loadtoXCOM,
            op_kwargs={"command": command},
            trigger_rule = TriggerRule.ONE_SUCCESS
        )

        BashCommand= BashOperator(
            task_id=step+'_BashCommand',
            bash_command = f"{{{{ ti.xcom_pull(key='return_value', task_ids='{step + '_JsonRead'}') }}}}"
        )

        Decidewhichtask = BranchPythonOperator(
            task_id=step+'_Decidewhichtask',
            python_callable=decideWhichTask,
            op_kwargs={"step": step}
        )

        Dummy_pari = DummyOperator(
            task_id=step+'_dummy_pari'
        )

        Dummy_dispari = DummyOperator(
            task_id=step+'_dummy_dispari'
        )

        #unnecessary task because also jsonRead can have trigger_rule
        Check_final = DummyOperator(
            task_id=step+'_Check_final',
            trigger_rule = TriggerRule.ONE_SUCCESS
        )

        tasklist.append(jsonRead)
        tasklist.append(BashCommand)
        tasklist.append(Decidewhichtask)
        tasklist.append([Dummy_pari, Dummy_dispari])
        tasklist.append(Check_final)

    chain(*tasklist)
