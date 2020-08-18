import sys
import json
import logging
from airflow import DAG
from airflow import models
from datetime import datetime
from airflow.models import Variable
from airflow.models import BaseOperator
from datetime import datetime, timedelta
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
from airflow.hooks.mysql_hook import MySqlHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor

dag_source = DAG(
    dag_id='dag_sensor_source',
    start_date=datetime(2020, 1, 24, 0, 0),
    schedule_interval='* * * * *'
)

task_1 = DummyOperator(task_id='task_1', dag=dag_source)


dag_target = DAG(
    dag_id='dag_sensor_target',
    start_date=datetime(2020, 1, 24, 0, 0),
    schedule_interval='* * * * *'
)
task_sensor = ExternalTaskSensor(
    dag=dag_target,
    task_id='dag_sensor_source_sensor',
    retries=100,
    retry_delay=timedelta(seconds=30),
    mode='reschedule',
    external_dag_id='dag_sensor_source',
    external_task_id='task_1'
)

task_1 = DummyOperator(task_id='task_1', dag=dag_target)

task_sensor >> task_1