# Step - 1
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import logging

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
def load_data():
    logging.info("Loading Data")

def process_data():
    logging.info("Process Data")

def store_data():
    logging.info("Store Data")

# Step - 4
dag = DAG(dag_id='DAG-Multi-Step-Python-Operator', description='Multistep DAG with Python Operator', default_args=default_args, catchup=False, schedule_interval=timedelta(days=1))

# Step - 5
load_data = PythonOperator(task_id="scrape", python_callable=load_data)
process_data = PythonOperator(task_id="scrape", python_callable=process_data)
store_data = PythonOperator(task_id="scrape", python_callable=store_data)

# Step - 6
load_data >> process_data >> store_data
