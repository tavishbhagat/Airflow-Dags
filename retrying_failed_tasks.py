from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import random

dag = DAG('Retrying_Failed_Tasks', schedule_interval=None, start_date=days_ago(0), catchup=False, default_args={"owner": "Tavish"})

def first(**context):
    if random.random() < 0.5:
       raise Exception('Task failed because condition didn\'t satisfy!')
    print('This is GOOD!')

print_message = PythonOperator(
    task_id="print_message",
    python_callable=first,
    provide_context=True,
    retries=10,
    retry_delay=timedelta(seconds=5),
    dag=dag
)

dummy_task = DummyOperator(
    task_id="dummy_task",
    dag=dag
)

print_message >> dummy_task