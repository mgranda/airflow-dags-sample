# Step - 1
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

# Step - 2
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 0,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retry_delay': timedelta(minutes=5),
    'tags': ['Data pipeline']
}

# Step - 3 Define functions
def process_data(items):
    print(items)
    return 'Ok'

# Step - 4
dag = DAG(dag_id='DAG-Paralell_Python_Bash', description='Paralell DAG with Python Operator and Bash Operator', default_args=default_args, catchup=False, schedule_interval='0 0 * * *')

# Step - 5
tasks = [BashOperator(task_id='task_{0}'.format(t), bash_command='sleep 30'.format(t), dag=dag) for t in range(1, 5)]
task_5 = PythonOperator(task_id='task_5', python_callable=process_data, op_args=['Read 1000 items'], dag=dag)
task_6 = BashOperator(task_id='task_6', bash_command='sleep 3', dag=dag)
task_7 = BashOperator(task_id='task_7', bash_command='sleep 30', dag=dag)

# Step - 6
tasks >> task_5 >> task_6 >> task_7
