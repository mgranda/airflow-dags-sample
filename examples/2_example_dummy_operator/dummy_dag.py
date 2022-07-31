# Step - 1
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

# Step - 2
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 7, 31),
    'retries': 0
}

# Step - 3
dag = DAG(dag_id='DAG-1', default_args=default_args, catchup=False, schedule_interval='@once')

# Step - 4
start = DummyOperator(task_id='start', dag=dag)
end = DummyOperator(task_id='end', dag=dag)

# Step - 5
start >> end