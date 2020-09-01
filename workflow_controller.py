import os
import sys
import json
import logging
from airflow import DAG
from airflow import models
from airflow.hooks.mysql_hook import MySqlHook
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from datetime import datetime, timedelta
import pprint
pp = pprint.PrettyPrinter(indent=4)

SCHEDULE_INTERVAL = 3
SCHEDULE_LIMIT = 10
WORKFLOW_START_TASK = 'start'
BOOKMARK_START = 'start'

# Define the DAG
dag = DAG(dag_id='workflow_controller',
          default_args={"owner": "annguk", "start_date": datetime(2020, 9, 1)},
          schedule_interval=timedelta(seconds=2))

def conditionally_trigger(context, dag_run_obj):
    # c_p =context['params']['condition_param']
    """
    WF 마스터 정보
    """
    db = MySqlHook(mysql_conn_id='mariadb', schema="djob")
    sql = f"""
    select
        workflow_process_id,ngen,site_id,application_id,instance_id,schema_id,name,workflow_instance_id,state,retry_count,ready,
        execute_date,created_date,bookmark,version,request,reserved,message
    from
        workflow_process
    where 
        ready > 0 and retry_count < 10
    limit {SCHEDULE_LIMIT}
    """
    tasks = {}
    tasks[WORKFLOW_START_TASK] = []
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
            'message': ''
        }
        tasks[WORKFLOW_START_TASK].append(model)
        sql = f"""
        update workflow_process
            set ready = 0, bookmark = '{BOOKMARK_START}'
        where workflow_process_id = %s
        """
        db.run(sql, autocommit=True, parameters=[str(row[0])])   

    logging.info(f'check: {tasks[WORKFLOW_START_TASK]}')
    # 객체가 있는 경우 처리
    if tasks[WORKFLOW_START_TASK]:
        dag_run_obj.payload = {'message': str(tasks[WORKFLOW_START_TASK])}
        pp.pprint(dag_run_obj.payload)
        return dag_run_obj

# start = BashOperator(task_id='start', bash_command='echo start workflow controller', dag=dag)        

# Define the single task in this controller example DAG
trigger = TriggerDagRunOperator(task_id='workflow_dagrun',
                                trigger_dag_id="example_trigger_target_dag",
                                python_callable=conditionally_trigger,
                                params={'condition_param': True, 'message': '나의 이름은 이찬호입니다.'},
                                dag=dag)

# start >> trigger