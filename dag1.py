# dag1.py
import datetime
 
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
 
DAG_ID = "dag_1"
DEFAULT_ARGS = {'start_date': datetime.datetime(2019, 12, 15)}
 
with DAG(DAG_ID, default_args=DEFAULT_ARGS,
        schedule_interval="0 10 * * *") as dag:
   task_a = DummyOperator(task_id='task_a')
   task_b = DummyOperator(task_id='task_b')
   task_c = DummyOperator(task_id='task_c')
 
   [task_a, task_b] >> task_c