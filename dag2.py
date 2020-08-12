# dag2.py
import datetime

from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor

DAG_ID = "dag_2"
DEFAULT_ARGS = {'start_date': datetime.datetime(2019, 12, 15)}

with DAG(DAG_ID, default_args=DEFAULT_ARGS,
        schedule_interval="01 10 * * *") as dag:

   task_d = DummyOperator(task_id='task_d')
   task_e = DummyOperator(task_id='task_e')

   task_c_sensor = ExternalTaskSensor(
       task_id='task_c_sensor',
       external_dag_id='dag_1',
       external_task_id='task_c',
       allowed_states=['success'],
       execution_delta=datetime.timedelta(seconds=3))

   [task_d, task_c_sensor] >> task_e