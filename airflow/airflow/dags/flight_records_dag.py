from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import subprocess
import sys,os
# Arguments to be used during DAG exectuion 
default_args = {
    'owner': 'maxd',
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

def call_bash():
    os.chdir('/opt/airflow/python_code/')
    exit_code = subprocess.check_call([sys.executable, 'execution_flow.py'])
    print(exit_code)


# Functions that would be called during the DAG execution 

with DAG(
    dag_id='final_proj_test_dag',
    default_args=default_args,
    description='This dag will execute the complete ETL',
    start_date=datetime(2024,5,15,12),
    schedule_interval='@daily'
) as dag:
    # This is the task params and operator to be used during DAG schedualed execution 
    task1 = PythonOperator(
        task_id='flight_records_extraction_job',
        python_callable=call_bash
    )
    # This is the flow of the task 
    task1