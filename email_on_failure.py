from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.email import send_email
from airflow.utils.dates import days_ago
import random

dag = DAG('Email_On_Failure_Example', schedule_interval=None, start_date=days_ago(0), catchup=False, default_args={"owner": "Tavish"})

def notify_email(context):
    """Send custom email alerts."""

    task_instance = context['ti']
    task_id = task_instance.task_id
    dag_id = task_instance.dag_id
    exception = context.get('exception')

    print(exception)

    print(task_instance)

    title = "Airflow alert: " + dag_id + " DAG Failed"

    body = """
    Hi, <br>
    <br>
    There's been an error in the """ + task_id + """ job.<br><br>""" + str(exception) + """<br><br>
    Thanks,<br>
    Development Team <br>
    """

    send_email('tavish.bhagat@citiustech.com', title, body)

def first(**context):
    raise Exception('Task failed because condition didn\'t satisfy!')
    print('This is GOOD!')

task1 = PythonOperator(
    task_id='task1',
    python_callable=first,
    provide_context=True,
    on_failure_callback=notify_email,
    dag=dag
)