from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago


dag = DAG('DAG_2', schedule_interval='*/1 * * * *', start_date=days_ago(0), catchup=False)

def greeting():
    """Just check that the DAG is started in the log."""
    import logging
    logging.info('Hello World from DAG_2')

hello_python = PythonOperator(
    task_id='hello',
    python_callable=greeting,
    dag=dag)

goodbye_dummy = DummyOperator(task_id='goodbye',
            dag=dag)

hello_python >> goodbye_dummy