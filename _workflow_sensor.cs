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
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor

SCHEDULE_INTERVAL = 5
default_args = {
    'owner': 'annguk',
    'depends_on_past': False,
    'start_date': datetime(2020, 8, 14),
    'email': ['koreablaster@wizensoft.com'],
    'email_on_failure': False,
    'email_on_retry': False
}
WORKFLOW_PROCESS = 'workflow_process'

def get_workflow(**context):
    db = MySqlHook(mysql_conn_id='mariadb', schema="djob")
    sql = """
    select
        workflow_process_id,ngen,site_id,application_id,instance_id,schema_id,name,workflow_instance_id,state,retry_count,ready,
        execute_date,created_date,bookmark,version,request,reserved,message
    from
        workflow_process
    where 
        ready > 0 and retry_count < 10
    limit 10
    """
    tasks = {}
    tasks[WORKFLOW_PROCESS] = []
    rows = db.get_records(sql)
    for row in rows:
        model = {
            'workflow_process_id':row[0],
            'ngen':row[1],
            'site_id':row[2],
            'application_id':row[3],
            'instance_id':row[4],
            'schema_id':row[5],
            'name':row[6],
            'workflow_instance_id':row[7],
            'state':row[8],
            'retry_count':row[9],
            'ready':row[10],
            'execute_date':str(row[11]),
            'created_date':str(row[12]),
            'bookmark':row[13],
            'version':row[14],
            'request':row[15],
            'reserved':row[16],
            'message':row[17]
        }
        tasks[WORKFLOW_PROCESS].append(model)
        sql = f"""
        update workflow_process
            set ready = 0, bookmark = 'start'
        where workflow_process_id = %s
        """
        db.run(sql, autocommit=True, parameters=[str(row[0])])
    
    if tasks[WORKFLOW_PROCESS]:
        logging.info(f'djob.WORKFLOW_PROCESS find data')
    else:
        logging.info(f'djob.WORKFLOW_PROCESS empty data')

    # 객체가 있는 경우 처리
    return list(tasks.values())

with models.DAG("workflow_sensor", default_args=default_args, schedule_interval=timedelta(seconds=SCHEDULE_INTERVAL)) as dag:
    # Start workflow    
    task_sensor = ExternalTaskSensor(
        task_id='sensor_task',
        retries=100,
        retry_delay=timedelta(seconds=3),
        mode='reschedule',
        external_dag_id='workflow_source',
        external_task_id='task_1',
        dag=dag
        )
