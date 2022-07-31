# Step - 1
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

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
tasks = [BashOperator(task_id='task_{0}'.format(t), bash_command='sleep 30'.format(t)) for t in range(1, 5)]
task_5 = PythonOperator(task_id='task_4', python_callable=process_data, op_args=['Read 1000 items'])
task_6 = BashOperator(task_id='task_5', bash_command='echo "Pipeline Ok"')
task_7 = BashOperator(task_id='task_6', bash_command='sleep 30')

# Step - 6
tasks >> task_4 >> task_5 >> task_6