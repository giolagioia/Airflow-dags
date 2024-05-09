from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.dummy import DummyOperator
import pandas
import json

def applesaveCSV(**kwargs):
    apple_data = pandas.read_csv('https://query1.finance.yahoo.com/v7/finance/download/AAPL?period1=0&period2=9999999999&interval=1d&events=history')
    toBePushed=apple_data.tail(5)

    for index, row in toBePushed.iterrows():
        toBePushedRow = toBePushed.loc[[index]]
        toBePushedRowSerezialible = toBePushedRow.to_json(orient='records')
        valueJson = json.dumps(toBePushedRowSerezialible)

        rowDate= row['Date']
        kwargs['ti'].xcom_push(key=rowDate, value=valueJson)
    #apple_data.to_csv('C:\\Users\\g.lagioia\\docker\\airflow\\dags\\AAPL.csv')

def amazonsaveCSV(**kwargs):
    amazon_data = pandas.read_csv('https://query1.finance.yahoo.com/v7/finance/download/AMZN?period1=0&period2=9999999999&interval=1d&events=history')
    toBePushed = amazon_data.tail(1)
    toBePushedSerezialible= toBePushed.to_json(orient='records')
    valueJson = json.dumps(toBePushedSerezialible)
    kwargs['ti'].xcom_push(key="1", value=valueJson)
    #amazon_data.to_csv('C:\\Users\\g.lagioia\\docker\\airflow\\dags\\AMZN.csv')

def editCSV():
    apple_data = kwargs['ti'].xcom_pull(task_ids='apple_extract_data')
    amazon_data = kwargs['ti'].xcom_pull(task_ids='amazon_extract_data')
    apple_data['Comparison value'] = amazon_data['Close']
    for index, row in apple_data.iterrows():
        for key, value in row.items():
            kwargs['ti'].xcom_push(key=key, value=value)
    amazon_data = pandas.read_csv('C:\\Users\\g.lagioia\\docker\\airflow\\dags\\AMZN.csv')
    apple_data['Comparison value'] = amazon_data['Close']
    apple_data.to_csv('C:\\Users\\g.lagioia\\docker\\airflow\\dags\\AAPL_compared.csv')


with DAG(
    dag_id='dataPipeline_DAG',
    start_date=datetime(2024,5,5,0),
    catchup=False,
) as dag:

    AppleDataExtract = PythonOperator(
        task_id='apple_extract_data',
        python_callable=applesaveCSV,
    )

    AmazonDataExtract = PythonOperator(
        task_id='amazon_extract_data',
        python_callable=amazonsaveCSV,
    )

    CorrectExecution = DummyOperator(
        task_id='correct_execution',
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    AppleDataExtract >> AmazonDataExtract >> CorrectExecution