from airflow import DAG
from airflow.operators.sensors import ExternalTaskSensor
from airflow.operators.python_operator import PythonOperato
from airflow.utils.dates import days_ago

dag = DAG('Master_Dag', schedule_interval='*/1 * * * *', start_date=days_ago(0), catchup=False)

def greeting():
    print('Hello World from DAG MASTER')

externalsensor1 = ExternalTaskSensor(
    task_id='check_dag_1_status',
    external_dag_id='DAG_1',
    external_task_id=None,  # wait for whole DAG to complete
    check_existence=True,
    timeout=120,
    dag=dag)

externalsensor2 = ExternalTaskSensor(
    task_id='check_dag_2_status',
    external_dag_id='DAG_2',
    external_task_id=None,
    check_existence=True,
    timeout=120,
    dag=dag)

hello_from_master = PythonOperator(
    task_id='hello_from_master',
    python_callable=greeting,
    dag=dag)

[externalsensor1, externalsensor2] >> hello_from_master