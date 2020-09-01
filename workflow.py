import os
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
from airflow.operators.python_operator import BranchPythonOperator
from airflow.hooks.mysql_hook import MySqlHook
from airflow.operators.mysql_operator import MySqlOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
# from wizen_plugin.sensors.workflow_sensors import WorkflowSensor
from datetime import datetime, timedelta
from airflow.utils.helpers import chain
from cryptography.fernet import Fernet

from airflow.operators.subdag_operator import SubDagOperator
from wizen_plugin.operators.signers_operator import SignersOperator
# from wizen_plugin.subdag.signers_subdag import signers_subdag
 
# 스케줄 = 초'
SCHEDULE_INTERVAL = 30
SCHEDULE_LIMIT = 10
default_args = {
    'owner': 'annguk',
    'depends_on_past': False,
    'start_date': datetime(2020, 8, 19),
    'email': ['koreablaster@wizensoft.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    # 'retries': 1,
    # 'retry_delay': timedelta(minutes=1)
}
DISPLAY_MINUS = '----------'
GLOBALS = 1 # 공통
APPLICATION = 2 # 결재
CODE = 'code'
WORKFLOW_STATE = 'state'
# 테스크
WORKFLOW_START_TASK = 'start'
WORKFLOW_STATUS_TASK = 'status'
WORKFLOW_END_TASK = 'end'
INSTANCE_TASK = 'instances'
INSTANCE_STATUS_TASK = 'instances_status'
INSTANCE_COMPLETED_TASK = 'instances_completed'
SETTING_TASK = 'settings'
SIGNER_TASK = 'signers'
SIGNER_ING_TASK = 'signers_ing'
SIGNER_COMPLETED_TASK = 'signers_completed'
# WF 북마크
BOOKMARK_START = 'start'
BOOKMARK_INSTANCE = 'instance'
# 테이블
BOXES = 'boxes'
USERS = 'users'
GROUPS = 'groups'
GROUP_USERS = 'group_users'
SIGNERS = 'signers'
INSTANCES = 'instances'
WORKFLOWS = 'workflows'
WORKFLOW_PROCESS = 'workflow_process'
# 결재선
SIGN_AREAS = 'sign_areas'
SIGN_ACTIVITY = 'sign_activity'
SIGN_AREA = 'sign_area'
SIGN_SECTION = 'sign_section'
SIGN_POSITION = 'sign_position'
SIGN_ACTION = 'sign_action'
# 상태 정보
STATUS_00 = '00' # 기안
STATUS_01 = '01' # 진행중 : 결재
STATUS_02 = '02' # 완료
STATUS_03 = '03' # 반려
STATUS_04 = '04'
STATUS_05 = '05'
# 북마크
BOOKMARK_START = 'start'

def get_workflow(**context):
    """
    WF 마스터 정보
    """
    # key = "5EpGKTgEhjBn6cX67I20u0p2gUFznUAEbKYAh0ghlPw=" #Fernet.generate_key()
    # cipher_suite  = Fernet(key)
    # cipher_text = cipher_suite.encrypt(b"dnlwps1!")
    # plain_text = cipher_suite.decrypt(cipher_text)
    # logging.info(f'key: {key}')
    # logging.info(f'cipher_text: {cipher_text}')
    # logging.info(f'plain_text: {plain_text}')

    db = MySqlHook(mysql_conn_id='mariadb', schema="djob")
    # # wfp = context['ti'].xcom_pull(task_ids='wf_sensor_task', key=WORKFLOW_PROCESS)
    # # if wfp:
    # #     for row in wfp:
    # #         logging.info(f'wfp row {row}')
    # # else:
    # #     logging.info(f'WORKFLOW_PROCESS data is empty')

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
        # context['ti'].xcom_push(key=WORKFLOW_START_TASK, value=tasks[WORKFLOW_START_TASK])
        return list(tasks.values())

def get_status(**context):    
    """
    실행할 프로세스 확인
    """
    wf = context['ti'].xcom_pull(task_ids=WORKFLOW_START_TASK)
    # logging.info(f"return values: '{context['ti'].xcom_pull(task_ids=WORKFLOW_START_TASK)}'")
    if wf:
        task_id = INSTANCE_TASK
        logging.info(f'{DISPLAY_MINUS} 프로세스 실행 {DISPLAY_MINUS}')
    else:
        task_id = WORKFLOW_END_TASK
        logging.info(f'{DISPLAY_MINUS} 실행할 프로세스 없음 {DISPLAY_MINUS}')
    return task_id 

def set_error(workflow_process_id, message):
    """
    에러 등록
    """
    db = MySqlHook(mysql_conn_id='mariadb', schema="djob")
    sql = f"""
    update workflow_process
        set ready = 1, retry_count = retry_count + 1, message = %s
    where workflow_process_id = %s
    """
    db.run(sql, autocommit=True, parameters=[message, workflow_process_id])

def get_instance(**context):
    """
    결재 마스터 정보
    """
    # 실행할 프로세스 확인
    workflows = context['ti'].xcom_pull(task_ids=WORKFLOW_START_TASK)
    if workflows:
        db = MySqlHook(mysql_conn_id='mariadb', schema="dapp")
        tasks = {}
        tasks[INSTANCE_TASK] = []
        tasks[INSTANCE_COMPLETED_TASK] = []
        # logging.info(f'workflows: {DISPLAY_MINUS} {workflows} {DISPLAY_MINUS}')
        for wf in workflows[0]:
            logging.info(f'wf: {DISPLAY_MINUS} {wf} {DISPLAY_MINUS}')
            # logging.info(f'instance_id: {DISPLAY_MINUS} {wf["instance_id"]} {DISPLAY_MINUS}')
            instance_id = int(wf['instance_id'])
            sql = f"""
            select
                instance_id,state,form_id,parent_id,workflow_id,subject,creator_culture,creator_name,group_culture,group_name,is_urgency,is_comment,is_related_document,
                attach_count,summary,re_draft_group_id,sub_proc_group_id,interface_id,created_date,completed_date,
                creator_id,group_id
            from
                instances
            where 
                instance_id = %s
            """            
            rows = db.get_records(sql, parameters=[instance_id])
            for row in rows:
                state = row[1]
                model = {
                    'instance_id':row[0],
                    'state': state,
                    'form_id':row[2],
                    'parent_id':row[3],
                    'workflow_id':row[4],
                    'subject':row[5],
                    'creator_culture':row[6],
                    'creator_name':row[7],
                    'group_culture': row[8],
                    'group_name':row[9],
                    'is_urgency':row[10],
                    'is_comment':row[11],
                    'is_related_document':row[12],
                    'attach_count':row[13],
                    'summary':row[14],
                    're_draft_group_id':row[15],
                    'sub_proc_group_id':row[16],
                    'interface_id':row[17],
                    'created_date':str(row[18]),
                    'completed_date':str(row[19]),
                    'creator_id':row[20],
                    'group_id':row[21]
                }
                if state == STATUS_02:
                    tasks[INSTANCE_COMPLETED_TASK].append(model)
                else:
                    tasks[INSTANCE_TASK].append(model)

        # 완료된 프로세스가 실행될 경우
        if tasks[INSTANCE_COMPLETED_TASK]:
            context['ti'].xcom_push(key=INSTANCE_COMPLETED_TASK, value=tasks[INSTANCE_COMPLETED_TASK])

        if tasks[INSTANCE_TASK]:
            return list(tasks.values())
    else:
        logging.info(f'{DISPLAY_MINUS} 실행할 프로세스 없음 {DISPLAY_MINUS}')

def get_instance_status(**context):    
    """
    실행할 결재 프로세스 확인
    """
    rows = context['ti'].xcom_pull(task_ids=INSTANCE_TASK)
    if rows:
        task_id = SETTING_TASK
        logging.info(f'{DISPLAY_MINUS} 결재 프로세스 실행 {DISPLAY_MINUS}')
    else:
        task_id = WORKFLOW_END_TASK
        logging.info(f'{DISPLAY_MINUS} 실행할 결재 프로세스 없음 {DISPLAY_MINUS}')
    return task_id

def get_settings(**context):
    """
    설정 마스터
    """
    codes = get_codes(context)
    get_boxes(context)
    instances = context['ti'].xcom_pull(task_ids=INSTANCE_TASK)
    if instances:
        for row in instances[0]:
            logging.info(f'get_settings instances {row}')
    else:
        logging.info(f'{DISPLAY_MINUS} 데이터 없음 {DISPLAY_MINUS}')

def get_codes(context):
    """
    코드 
    """
    logging.info('get_codes')

def get_signers_ing(**context):
    """
    진행중인 프로세스 이벤트 처리
    """
    rows = context['ti'].xcom_pull(task_ids=SIGNER_TASK, key=SIGNER_ING_TASK)
    if rows:
        logging.info(f'진행중인 프로세스 이벤트 처리: {rows}')

def get_signers_completed(**context):
    """
    완료된 프로세스 이벤트 처리
    """
    rows = context['ti'].xcom_pull(task_ids=SIGNER_TASK, key=SIGNER_COMPLETED_TASK)
    if rows:
        logging.info(f'완료된 프로세스 이벤트 처리 rows: {rows}')
        logging.info(f'완료된 프로세스 이벤트 처리 rows[0]: {rows[0]}')
        for row in rows[0]:
            logging.info(f'완료된 프로세스 이벤트 처리: {row}')

def get_boxes(context):
    """
    결재함
    """
    db = MySqlHook(mysql_conn_id='mariadb', schema="dapp")
    sql = f"""
    select 
        box_id, box_type, parent_id, name, value, sequence, is_display, is_mobile, is_security, is_related_doc, is_count, icon_name, remark 
    from dapp.boxes
    """
    tasks = {}
    rows = db.get_records(sql)
    tasks[BOXES] = []
    for row in rows:
        model = {
            'box_id':row[0],
            'box_type':row[1],
            'parent_id':row[2],
            'name':row[3],
            'value':row[4],
            'sequence':row[5],
            'is_display':row[6],
            'is_mobile':row[7],
            'is_security': row[8],
            'is_related_doc':row[9],
            'is_count':row[10],
            'icon_name':str(row[11]),
            'remark':str(row[12])
        }
        tasks[BOXES].append(model)

    result = str(tasks[BOXES])
    # context['ti'].xcom_push(key=BOXES, value=tasks[BOXES])
    # logging.info(f'result: {result}')
    os.system(f'airflow variables --set boxes "' + result + '"')
    # return list(tasks.values())

# timedelta(seconds=SCHEDULE_INTERVAL))
with models.DAG("workflow", default_args=default_args, schedule_interval=None) as dag:
    # Watch workflow process
    # wf_sensor = ExternalTaskSensor(
    #     task_id='wf_sensor_task',
    #     retries=100,
    #     retry_delay=timedelta(seconds=30),
    #     mode='reschedule',
    #     external_dag_id='workflow_sensor',
    #     external_task_id='start_task',
    #     dag=dag
    # )
    
    # wf_sensor = WorkflowSensor(task_id='wf_sensor_task', poke_interval=3, mode='reschedule', retry_delay=timedelta(seconds=1), dag=dag)
    # Start workflow    
    wf_start = PythonOperator(task_id=WORKFLOW_START_TASK, python_callable=get_workflow, provide_context=True, dag=dag)
    # Status get
    wf_status = BranchPythonOperator(task_id=WORKFLOW_STATUS_TASK,python_callable=get_status,provide_context=True,dag=dag)
    # wf_status = PythonOperator(task_id='wf_status',python_callable=get_status,provide_context=True,dag=dag)
    # End workflow    
    wf_end = BashOperator(task_id=WORKFLOW_END_TASK,bash_command='echo 실행할 프로세스 없음',dag=dag)

    # instances
    instances = PythonOperator(task_id=INSTANCE_TASK,python_callable=get_instance, provide_context=True, dag=dag)
    # 실행할 결재 프로세스 확인
    instances_status = BranchPythonOperator(task_id=INSTANCE_STATUS_TASK,python_callable=get_instance_status,provide_context=True,dag=dag)

    # Setting
    settings = PythonOperator(task_id=SETTING_TASK,python_callable=get_settings, provide_context=True, dag=dag)

    # Status: 상태 '{{ ti.xcom_pull(task_ids='load_config', dag_id='workflow' }}'
    signers = SignersOperator(task_id=SIGNER_TASK, wf_start_task=WORKFLOW_START_TASK, instance_task=INSTANCE_TASK, setting_task=SETTING_TASK, provide_context=True, dag=dag)
    
    # 진행중인 프로세스 이벤트 처리
    signers_ing = PythonOperator(task_id=SIGNER_ING_TASK,python_callable=get_signers_ing,provide_context=True,dag=dag)    

    # 완료된 프로세스 이벤트 처리
    signers_completed = PythonOperator(task_id=SIGNER_COMPLETED_TASK,python_callable=get_signers_completed,provide_context=True,dag=dag)    

    # status = PythonOperator(task_id='status_task',python_callable=branch_status, provide_context=True, dag=dag)
    # # 00: 기안
    # status_00 = PythonOperator(task_id='status_00_task',python_callable=get_status_00, provide_context=True, dag=dag)
    # # 01: 현결재 true || false
    # status_01 = BashOperator(task_id='status_01_task',bash_command='echo get status 현결재 true || false',dag=dag)

    # # Area: 결재영역
    # area = BashOperator(task_id='area_task', bash_command='echo get area', dag=dag)
    # # 00: 기안회수
    # area_00 = BashOperator(task_id='area_00_task', bash_command='echo get area_00 기안회수', dag=dag)    
    # # 01: 일반결재
    # area_01 = BashOperator(task_id='area_01_task', bash_command='echo get area_01 일반결재', dag=dag)
    # # 02: 수신결재
    # area_02 = BashOperator(task_id='area_02_task', bash_command='echo get area_02 수신결재', dag=dag)
    # # 03: 부서합의
    # area_03 = BashOperator(task_id='area_03_task', bash_command='echo get area_03 부서합의', dag=dag)

    # # Section: 결재구분
    # section = BashOperator(task_id='section_task', bash_command='echo get section 결재구분', dag=dag)
    # # 00: 기안결재
    # section_00 = BashOperator(task_id='section_00_task', bash_command='echo get section_00 기안결재', dag=dag)
    # # 01: 일반결재
    # section_01 = BashOperator(task_id='section_01_task', bash_command='echo get section_01 일반결재', dag=dag)
    # # 02: 병렬합의
    # section_02 = BashOperator(task_id='section_02_task', bash_command='echo get section_02 병렬합의', dag=dag)
    # # 03: 순차합의
    # section_03 = BashOperator(task_id='section_03_task', bash_command='echo get section_03 순차합의', dag=dag)

    # # Process: 결재 프로세스 처리(결재선 & 결재함)
    # process = BashOperator(task_id='process_task', bash_command='echo get process 결재 프로세스 처리(결재선 & 결재함)', dag=dag)

    # # Event: 결재 이벤트 처리
    # event = BashOperator(task_id='event_task', bash_command='echo get event 결재 이벤트 처리', dag=dag)
    # # 00: 진행중
    # event_00 = BashOperator(task_id='event_00_task', bash_command='echo get event_00 진행중 이벤트 처리', dag=dag)
    # # 01: 완료
    # event_01 = BashOperator(task_id='event_01_task', bash_command='echo get event_01 완료 이벤트 처리', dag=dag)

    # # Notify: 알림 이벤트 처리
    # notify = BashOperator(task_id='notify_task', bash_command='echo get notify 알림 이벤트 처리', dag=dag)
    # # 00: 이메일
    # notify_00 = BashOperator(task_id='notify_00_task', bash_command='echo get notify_00 이메일 알림 처리', dag=dag)
    # # 01: 쪽지
    # notify_01 = BashOperator(task_id='notify_01_task', bash_command='echo get notify_01 쪽지 알림 처리', dag=dag)
    # # 02: 모바일
    # notify_02 = BashOperator(task_id='notify_02_task', bash_command='echo get notify_02 모바일 알림 처리', dag=dag)

    # # Doc: 문서발번
    # doc = BashOperator(task_id='doc_task', bash_command='echo get doc 문서발번 처리', dag=dag)

    # # Auth: 권한처리
    # # auth = BashOperator(task_id='auth_task', bash_command='echo get auth 권한 처리', dag=dag)

    # # Complete: 완료처리
    # complete = BashOperator(task_id='complete_task', bash_command='echo get complete 완료처리', dag=dag)


    ##
    # Workflow Start
    # 실행할 프로세스가 없는 경우
    wf_start >> wf_status >> wf_end
    # 실행할 결재 프로세스가 없는 경우
    wf_status >> instances >> instances_status >> wf_end
    # 프로세스 실행
    instances_status >> settings >> signers >> [signers_ing, signers_completed]
    # # 결재중이 아니면 완료 처리
    # instances >> complete
    
    # # 결재상태
    # status >> status_00 >> section
    # status >> status_01

    # # 결재영역 >> 결재구분 >> 결재 프로세스 >> 결재 이벤트
    # status_01 >> area >> [area_00, area_01, area_02, area_03] >> section >> [section_00, section_01, section_02, section_03] >> process >> event

    # # 결재 이벤트 >> 대상자 알림 >> 결재 엔진 상태 종료
    # event >> [event_00, event_01] >> complete >> doc >> notify >> [notify_00, notify_01, notify_02] >> wf_end





# # # 결재선
# # def get_signers(instance_id, context):
# #     db = MySqlHook(mysql_conn_id='mariadb', schema="dapp")
# #     sql = f"""
# #     select
# #         instance_id, sign_area_id, sequence, sub_instance_id, sign_section, sign_position, sign_action, is_executed, group_culture, group_id, group_name, 
# #         created_date, received_date, approved_date
# #     from
# #         signers
# #     where 
# #         instance_id = %s
# #     """
# #     tasks = {}
# #     rows = db.get_records(sql, parameters=[instance_id])
# #     tasks[SIGNERS] = []
# #     for row in rows:
# #         model = {
# #             'instance_id':row[0],
# #             'sign_area_id':row[1],
# #             'sequence':row[2],
# #             'sub_instance_id':row[3],
# #             'sign_section':row[4],
# #             'sign_position':row[5],
# #             'sign_action':row[6],
# #             'is_executed':row[7],
# #             'group_culture': row[8],
# #             'group_id':row[9],
# #             'group_name':row[10],
# #             'created_date':str(row[11]),
# #             'received_date':str(row[12]),
# #             'approved_date':str(row[13])
# #         }
# #         tasks[SIGNERS].append(model)

# #     context['ti'].xcom_push(key=SIGNERS, value=tasks[SIGNERS])
# #     return list(tasks.values())

# # 결재선 등록
# def set_signers(doc, group, context):
#     db = MySqlHook(mysql_conn_id='mariadb', schema="dapp")
#     sql = f"""
#     insert into signers(instance_id, sign_area_id, sequence, sub_instance_id, sign_section, sign_position, sign_action, is_executed, group_culture, group_id, group_name, 
#         created_date, received_date, approved_date)
#     values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
#     """
#     is_executed = True
#     sub_instance_id = 0
#     sign_action = STATUS_00 # 기결재
#     db.run(sql, autocommit=True, parameters=[
#         doc.find('instance_id').text,
#         doc.find('sign_area_id').text,
#         doc.find('sequence').text,
#         sub_instance_id,
#         doc.attrib['sign_section'],
#         doc.attrib['sign_position'],
#         sign_action,
#         is_executed,
#         group['culture'],
#         doc.find('group_id').text,
#         group['name'],
#         datetime.now(),
#         datetime.now(),
#         datetime.now()])


# # 결재선 사용자 등록
# def set_sign_users(doc, context):
#     db = MySqlHook(mysql_conn_id='mariadb', schema="dapp")
#     # conn = db.get_conn()
#     # cursor = conn.cursor()
#     sql = f"""
#     insert into sign_users(instance_id, sign_area_id, sequence, user_culture, user_id, user_name, responsibility, position, class_position, host_address, reserved_date, 
#     delay_time, is_deputy, is_comment)
#     values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
#     """
#     # result = cursor.execute(sql)
#     # logging.info(f'cursor result: {result}')

#     db.run(sql, autocommit=True, parameters=[
#         doc.find("instance_id").text,
#         doc.find('sign_area_id').text,
#         doc.find('sequence').text,
#         doc.find('user_culture').text,
#         doc.find('user_id').text,
#         doc.find('user_name').text,
#         doc.find('responsibility').text,
#         doc.find('position').text,
#         doc.find('class_position').text,
#         doc.find('host_address').text,
#         doc.find('reserved_date').text,
#         doc.find('delay_time').text,
#         doc.find('is_deputy').text,
#         doc.find('is_comment').text])