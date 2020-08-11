import airflow
from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator

args = {
    'owner': 'Airflow',
    'start_date': airflow.utils.dates.days_ago(2),
}

DAG = DAG(
  dag_id='ex_xcom',
  default_args=args,
#  start_date=datetime(2019, 04, 21),
  schedule_interval="@daily",
  #schedule_interval=timedelta(1)
)

def push_function(**context):
    msg='메시지'
    print("message to push: '%s'" % msg)
    task_instance = context['task_instance']
    task_instance.xcom_push(key="the_message", value=msg)

push_task = PythonOperator(
    task_id='push_task', 
    python_callable=push_function,
    provide_context=True,
    dag=DAG)

def pull_function(**kwargs):
    ti = kwargs['ti']
    msg = ti.xcom_pull(task_ids='push_task',key='the_message')
    print("received message: '%s'" % msg)

pull_task = PythonOperator(
    task_id='pull_task', 
    python_callable=pull_function,
    provide_context=True,
    dag=DAG)

push_task >> pull_task