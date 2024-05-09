from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
import pandas

def applesaveCSV():
    apple_data = pandas.read_csv('https://query1.finance.yahoo.com/v7/finance/download/AAPL?period1=0&period2=9999999999&interval=1d&events=history')
    apple_data.to_csv('C:\\Users\\g.lagioia\\docker\\airflow\\dags\\AAPL.csv')

def amazonsaveCSV():
    amazon_data = pandas.read_csv('https://query1.finance.yahoo.com/v7/finance/download/AMZN?period1=0&period2=9999999999&interval=1d&events=history')
    amazon_data.to_csv('C:\\Users\\g.lagioia\\docker\\airflow\\dags\\AMZN.csv')

def editCSV():
    apple_data = pandas.read_csv('C:\\Users\\g.lagioia\\docker\\airflow\\dags\\AAPL.csv')
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

    DataTransform = PythonOperator(
        task_id='transform_data',
        python_callable=editCSV,
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    AppleDataExtract >> AmazonDataExtract >> DataTransform