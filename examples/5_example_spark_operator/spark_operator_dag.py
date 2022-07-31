# Step - 1
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
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
    'tags': ['Data pipeline'],
    'catchup': False
}

# Step - 3 Define functions
def process_data(items):
    print(items)
    return 'Ok'

# Step - 4
dag = DAG(dag_id='DAG-Spark', description='DAG with Spark Operator', default_args=default_args, schedule_interval=timedelta(days=1))

# Step - 5
t1 = SparkKubernetesOperator(
   task_id='spark_pi_submit',
   namespace="spark-apps",
   application_file="example_spark_kubernetes_spark_pi.yaml",
   do_xcom_push=True,
   dag=dag,
)

t2 = SparkKubernetesSensor(
   task_id='spark_pi_monitor',
   namespace="spark-apps",
   application_name="{{ task_instance.xcom_pull(task_ids='spark_pi_submit')['metadata']['name'] }}",
   dag=dag,
)

# Step - 6
t1 >> t2