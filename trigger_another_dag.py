from airflow import DAG
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.utils.dates import days_ago

dag = DAG('Trigger_Messaging_DAG', schedule_interval=None, start_date=days_ago(0), catchup=False, default_args={"owner": "Tavish"})

def send_message(context, dag_run_obj):
    dag_run_obj.payload = { 'ts': context['ts'] }
    return dag_run_obj

trigger = TriggerDagRunOperator(
    task_id="test_trigger_dag",
    trigger_dag_id="Messaging_DAG",
    python_callable=send_message,
    params={ 'ts': "{{ dag_run.conf[‘ts’] }}" },
    dag=dag,
)
