# Import all the modules required
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

#Add default arguments
default_args = {
            "owner": "Tavish",
            "start_date": datetime(2020, 12, 15)
            }

#Instantiate the DAG
dag = DAG('bash_operator_example_dag', schedule_interval="50 01 * * *", catchup=False, default_args=default_args)

#Global variables
create_directory_command = "sh /home/vagrant/airflow/scripts/mkdir_command.sh "
create_file_command = "sh /home/vagrant/airflow/scripts/create_file.sh "

# Task 1
t1 = BashOperator(
            task_id= 'create_folder',
            bash_command=create_directory_command,
            dag=dag
        )
# Task 2
t2 = BashOperator(
            task_id= 'create_file',
            bash_command=create_file_command,
            dag=dag
        )

#Sequence of running the tasks
t1 >> t2