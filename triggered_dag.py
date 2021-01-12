from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago


dag = DAG('Messaging_DAG', schedule_interval=None, start_date=days_ago(0), catchup=False, default_args={"owner": "Tavish"})

def printMessageFromOtherDAG(**context):
    print(context)
    print("Received the timestamp : {}".format(context["dag_run"].conf["ts"]))
    ds = context["dag_run"].conf["ts"]

print_task = PythonOperator(
    task_id="print_message",
    python_callable=printMessageFromOtherDAG,
    provide_context=True,
    dag=dag
)
