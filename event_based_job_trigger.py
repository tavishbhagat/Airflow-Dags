from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.sensors import HdfsSensor
from airflow.operators.bash_operator import BashOperator
from airflow import DAG
from datetime import datetime, timedelta
import json

default_args = {
            "owner": "Tavish",
            "start_date": datetime(2020, 6, 15)
        }

dag = DAG('hdfs_sensor_example_dag', schedule_interval=None, catchup=False, default_args=default_args)

Sensing_HDFS_Folder = HdfsSensor(
    task_id='Sensing_HDFS_Folder',
    filepath = '/input_file',
    hdfs_conn_id='hdfs_default',
    timeout=60,
    dag=dag
)

move_file_command = "sh /home/vagrant/airflow/scripts/move_file_command.sh "
Moving_Input_File = BashOperator(
            task_id= 'Moving_Input_File',
            bash_command=move_file_command,
            dag=dag
        )

_config = {
        'name': 'Running_Word_Count_Spark_Job',
        'env_vars': {
                'AIRFLOW_APP_OWNER': 'Tavish'
        },
        'verbose': True,
        'application': '{{ var.json.airflow_vars.word_count_jar_path }}',
        'java_class': 'com.citiustech.word_count.WordCount'
}

Running_Word_Count_Spark_Job = SparkSubmitOperator(
        task_id='Running_Word_Count_Spark_Job',
        conn_id='spark_default',
        dag=dag,
        **_config)


Sensing_HDFS_Folder >> Moving_Input_File >> Running_Word_Count_Spark_Job