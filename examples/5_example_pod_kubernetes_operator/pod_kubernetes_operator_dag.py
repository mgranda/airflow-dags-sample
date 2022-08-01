# Step - 1
from datetime import datetime, timedelta
from airflow import DAG
from airflow.contrib.operators import kubernetes_pod_operator
from airflow.utils.dates import days_ago

# Step - 2
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retry_delay': timedelta(seconds=5),
    'tags': ['Data pipeline'],
    'catchup': False
}

# Step - 3
dag = DAG(dag_id='DAG-Pod-Kubernetes', description='DAG with Pod Kubernetes Operator', default_args=default_args, schedule_interval=None)

# Step - 4
extract_tranform = kubernetes_pod_operator.KubernetesPodOperator(
        namespace='airflow',
        image="python:3.7-slim",
        cmds=["echo"],
        arguments=["This can be the extract part of an ETL"],
        labels={"app": "etl-app"},
        name="extract-tranform",
        task_id="extract-tranform",
        get_logs=True
)

# Step - 5
extract_tranform