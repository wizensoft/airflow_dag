import sys
import json
import logging
from airflow import DAG
from airflow import models
import xml.etree.ElementTree as ET
from airflow.models import Variable
# foo = Variable.get("foo")
# bar = Variable.get("bar", deserialize_json=True)
# baz = Variable.get("baz", default_var=None)
from airflow.operators.bash_operator import BashOperator
# from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.mysql_hook import MySqlHook
from airflow.operators.mysql_operator import MySqlOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
# from wizen_plugin.sensors.workflow_sensors import WorkflowSensor
from datetime import datetime, timedelta
from airflow.utils.helpers import chain

default_args = {
    'owner': 'annguk',
    'depends_on_past': False,
    'start_date': datetime(2020, 8, 18),
    'email': ['koreablaster@wizensoft.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    # 'retries': 1,
    # 'retry_delay': timedelta(minutes=1)
}

with models.DAG("workflow_source", default_args=default_args, schedule_interval='* * * * *') as dag:
    task_1 = BashOperator(task_id='task_1',bash_command='echo wizen workflow start ',dag=dag)