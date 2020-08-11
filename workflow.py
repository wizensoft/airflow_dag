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
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.mysql_hook import MySqlHook
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators import WorkflowSensor
from datetime import datetime, timedelta
from airflow.utils.helpers import chain

default_args = {
    'owner': 'annguk',
    'depends_on_past': False,
    'start_date': datetime(2020, 1, 1),
    'email': ['koreablaster@wizensoft.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}
GLOBALS = 1 # 공통
APPLICATION = 2 # 결재
CODE = 'code'
WORKFLOW_STATE = 'state'
# 테스크
WORKFLOW_START_TASK = 'wf_start_task'
INSTANCE_TASK = 'instances_task'
# WF 북마크
BOOKMARK_START = 'start'
BOOKMARK_INSTANCE = 'instance'
# 테이블
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
STATUS_03 = '03'
STATUS_04 = '04'
STATUS_05 = '05'

# WF 마스터 정보
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
    limit 1
    """
    task = {}
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
        task = model

    # 객체가 있는 경우 처리
    if task != {}:
        context['ti'].xcom_push(key=WORKFLOWS, value=task)
        sql = f"""
        update workflow_process
            set ready = 0, bookmark = 'start'
        where workflow_process_id = %s
        """
        db.run(sql, autocommit=True, parameters=[task['workflow_process_id']])

    # return task
    
def start_workflow():
    db = MySqlHook(mysql_conn_id='mariadb', schema="djob")
    wp = context['ti'].xcom_pull(task_ids='wf_sensor_task', key=WORKFLOW_PROCESS)


# 에러 등록
def set_error(workflow_process_id, message):
    db = MySqlHook(mysql_conn_id='mariadb', schema="djob")
    sql = f"""
    update workflow_process
        set ready = 1, retry_count = retry_count + 1, message = %s
    where workflow_process_id = %s
    """
    db.run(sql, autocommit=True, parameters=[message, workflow_process_id])

# 결재 마스터 정보
def get_instance(**context):
    workflow = context['ti'].xcom_pull(task_ids=WORKFLOW_START_TASK, key=WORKFLOWS)
    db = MySqlHook(mysql_conn_id='mariadb', schema="dapp")
    instance_id = int(workflow["instance_id"])
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
    task = {}
    rows = db.get_records(sql, parameters=[instance_id])
    for row in rows:
        model = {
            'instance_id':row[0],
            'state':row[1],
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
        task = model

    context['ti'].xcom_push(key=INSTANCES, value=task)
    return task
    
# 코드 
def get_codes():
    logging.info('get_codes')

# 결재 예정 정보
def get_sign_activity(instance_id, context):
    db = MySqlHook(mysql_conn_id='mariadb', schema="dapp")
    sql = f"""
    select
        instance_id, sign_area_id, sequence, sign_area, sign_section, sign_position, sign_action, is_comment, is_executed, group_id, user_id, host_address
    from
        sign_activity
    where 
        instance_id = %s
    """
    tasks = {}
    tasks[SIGN_ACTIVITY] = []
    rows = db.get_records(sql, parameters=[instance_id])
    for row in rows:
        model = {
            'instance_id':row[0],
            'sign_area_id':row[1],
            'sequence':row[2],
            'sign_area':row[3],
            'sign_section':row[4],
            'sign_position':row[5],
            'sign_action':row[6],
            'is_comment':row[7],
            'is_executed':row[8],
            'group_id':row[9],
            'user_id':row[10],
            'host_address':row[11]
        }
        tasks[SIGN_ACTIVITY].append(model)

    context['ti'].xcom_push(key=SIGN_ACTIVITY, value=tasks[SIGN_ACTIVITY])
    return list(tasks.values())

# 결재선
def get_signers(instance_id, context):
    db = MySqlHook(mysql_conn_id='mariadb', schema="dapp")
    sql = f"""
    select
        instance_id, sign_area_id, sequence, sub_instance_id, sign_section, sign_position, sign_action, is_executed, group_culture, group_id, group_name, 
        created_date, received_date, approved_date
    from
        signers
    where 
        instance_id = %s
    """
    tasks = {}
    rows = db.get_records(sql, parameters=[instance_id])
    tasks[SIGNERS] = []
    for row in rows:
        model = {
            'instance_id':row[0],
            'sign_area_id':row[1],
            'sequence':row[2],
            'sub_instance_id':row[3],
            'sign_section':row[4],
            'sign_position':row[5],
            'sign_action':row[6],
            'is_executed':row[7],
            'group_culture': row[8],
            'group_id':row[9],
            'group_name':row[10],
            'created_date':str(row[11]),
            'received_date':str(row[12]),
            'approved_date':str(row[13])
        }
        tasks[SIGNERS].append(model)

    context['ti'].xcom_push(key=SIGNERS, value=tasks[SIGNERS])
    return list(tasks.values()) 

# 결재선 등록
def set_signers(doc, group, context):
    db = MySqlHook(mysql_conn_id='mariadb', schema="dapp")
    sql = f"""
    insert into signers(instance_id, sign_area_id, sequence, sub_instance_id, sign_section, sign_position, sign_action, is_executed, group_culture, group_id, group_name, 
        created_date, received_date, approved_date)
    values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
    """
    is_executed = True
    sub_instance_id = 0
    sign_action = STATUS_00 # 기결재
    db.run(sql, autocommit=True, parameters=[
        doc.find('instance_id').text,
        doc.find('sign_area_id').text,
        doc.find('sequence').text,
        sub_instance_id,
        doc.attrib['sign_section'],
        doc.attrib['sign_position'],
        sign_action,
        is_executed,
        group['culture'],
        doc.find('group_id').text,
        group['name'],
        datetime.now(),
        datetime.now(),
        datetime.now()])

# 결재선 등록 스크립트
def insert_signers_sql(lst, group, context):
    sql = 'insert into signers(instance_id, sign_area_id, sequence, sub_instance_id, sign_section, sign_position, sign_action, is_executed, group_culture, group_id, group_name, created_date, received_date, approved_date) values'
    index = 0
    is_executed = 1
    sub_instance_id = 0
    sign_action = STATUS_00 # 기결재    
    for m in lst:
        index += 1
        colon = ''
        if len(lst) > index:        
            colon = ','
        sql += """({},{},'{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}'){}""".format(
            m['instance_id'],
            m['sign_area_id'],
            m['sequence'],
            sub_instance_id,
            m['sign_section'],
            m['sign_position'],
            sign_action,
            is_executed,
            group['culture'],
            m['group_id'],
            group['name'],
            datetime.now(),
            datetime.now(),
            datetime.now(),
            colon)
        sql += ';'
    return sql

# 결재진행함 스크립트
def insert_post_office_sql(lst, context):
    sql = 'insert into post_office(box_id,participant_id,instance_id,row_version,name,participant_name,is_viewed,created_date) values'
    index = 0
    is_viewed = 0
    for m in lst:
        index += 1
        colon = ''
        if len(lst) > index:        
            colon = ','
        sql += """({},'{}',{},'{}','{}','{}','{}','{}'){}""".format(
            m['box_id'],
            m['participant_id'],
            m['instance_id'],
            m['row_version'],
            m['name'],
            m['participant_name'],
            is_viewed,
            datetime.now(),
            colon)
        sql += ';'
    return sql
# 결재완료함 스크립트
def insert_post_boxes_sql(lst, context):
    sql = 'insert into post_boxes(box_id,participant_id,instance_id,row_version,name,participant_name,is_viewed,created_date) values'
    index = 0
    is_viewed = 0
    for m in lst:
        index += 1
        colon = ''
        if len(lst) > index:        
            colon = ','
        sql += """({},'{}',{},'{}','{}','{}','{}','{}'){}""".format(
            m['box_id'],
            m['participant_id'],
            m['instance_id'],
            m['row_version'],
            m['name'],
            m['participant_name'],
            is_viewed,
            datetime.now(),
            colon)
        sql += ';'
    return sql    
# 결재선 사용자 등록 스크립트
def insert_sign_users_sql(lst, activity, context):
    sql = """insert into sign_users(instance_id, sign_area_id, sequence, user_culture, user_id, user_name, responsibility, position, class_position, host_address, reserved_date, delay_time, is_deputy, is_comment) values"""
    index = 0
    for m in lst:
        index += 1
        colon = ''
        if len(lst) > index:        
            colon = ','
        uid = m['user_id']
        gid = m['group_id']
        m['responsibility'] = ''
        m['position'] = ''
        m['class_position'] = ''
        if activity['host_address'] == None:
            activity['host_address'] = ''
        m['reserved_date'] = 'null'
        m['delay_time'] = 0
        m['is_deputy'] = 0
        group_users = get_group_users(gid, uid, context)
        if group_users:
            for n in group_users[0]:
                if n['relation_type'] == 1:
                    m['responsibility'] = n['group_id']
                elif n['relation_type'] == 2:
                    m['position'] = n['group_id']
                elif n['relation_type'] == 3:
                    m['class_position'] = n['group_id']
        sql += """({},{},'{}','{}','{}','{}','{}','{}','{}','{}',{},'{}',{},{}){}""".format(
            activity['instance_id'],
            activity['sign_area_id'],
            activity['sequence'],
            m['culture'],
            m['user_id'],
            m['name'], 
            m['responsibility'],
            m['position'],
            m['class_position'],
            activity['host_address'],
            m['reserved_date'],
            m['delay_time'],
            m['is_deputy'],
            activity['is_comment'],
            colon)
    sql += ';'
    return sql
# 결재선 사용자 등록
def set_sign_users(doc, context):
    db = MySqlHook(mysql_conn_id='mariadb', schema="dapp")
    # conn = db.get_conn()
    # cursor = conn.cursor()
    sql = f"""
    insert into sign_users(instance_id, sign_area_id, sequence, user_culture, user_id, user_name, responsibility, position, class_position, host_address, reserved_date, 
    delay_time, is_deputy, is_comment)
    values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """
    # result = cursor.execute(sql)
    # logging.info(f'cursor result: {result}')

    db.run(sql, autocommit=True, parameters=[
        doc.find("instance_id").text,
        doc.find('sign_area_id').text,
        doc.find('sequence').text,
        doc.find('user_culture').text,
        doc.find('user_id').text,
        doc.find('user_name').text,
        doc.find('responsibility').text,
        doc.find('position').text,
        doc.find('class_position').text,
        doc.find('host_address').text,
        doc.find('reserved_date').text,
        doc.find('delay_time').text,
        doc.find('is_deputy').text,
        doc.find('is_comment').text])


# 사용자 정보
def get_users(user_id, context):
    db = MySqlHook(mysql_conn_id='mariadb', schema="dbo")
    sql = f"""
    select
        user_id, name, culture, group_id, employee_num, anonymous_name, email, theme_code, date_format_code, time_format_code, time_zone, row_count, language_code, 
        interface_id, phone, mobile, fax, icon, addsign_img, is_plural, is_notification, is_absence, is_deputy 
    from
        users
    where 
        user_id = %s
    """
    task = {}
    rows = db.get_records(sql, parameters=[user_id])
    for row in rows:
        model = {
            'user_id':row[0],
            'name':row[1],
            'culture':row[2],
            'group_id':row[3],
            'employee_num':row[4],
            'anonymous_name':row[5],
            'email':row[6],
            'theme_code':row[7],
            'date_format_code': row[8],
            'time_format_code':row[9],
            'row_count':row[10],
            'language_code':row[11],
            'interface_id':row[12],
            'phone':row[13],
            'mobile':row[14],
            'fax':row[15],
            'icon':row[16],
            'addsign_img':row[17],
            'is_plural':row[18],
            'is_notification':row[19],
            'is_absence':row[20],
            'is_deputy':row[21]
        }
        task = model

    context['ti'].xcom_push(key=USERS, value=task)
    return task

# 그룹 정보
def get_groups(group_id, context):
    db = MySqlHook(mysql_conn_id='mariadb', schema="dbo")
    sql = f"""
    select
        group_id, parent_id, name, culture, group_code, sequence, is_childs, depth, is_display, email, interface_id, remark, created_date, modified_date 
    from
        groups
    where 
        group_id = %s
    """
    task = {}
    rows = db.get_records(sql, parameters=[group_id])
    for row in rows:
        model = {
            'group_id':row[0],
            'parent_id':row[1],
            'name':row[2],
            'culture':row[3],
            'group_code':row[4],
            'sequence':row[5],
            'is_childs':row[6],
            'depth':row[7],
            'is_display': row[8],
            'email':row[9],
            'remark':row[10],
            'created_date':str(row[11]),
            'modified_date':str(row[12])
        }
        task = model

    context['ti'].xcom_push(key=GROUPS, value=task)
    return task    

# 그룹에 소속된 사용자 정보
def get_group_users(gid, uid, context):
    db = MySqlHook(mysql_conn_id='mariadb', schema="dbo")
    sql = f"""
    select 
        b.*, a.relation_type, a.is_master
    from 
        dbo.group_users a
        inner join dbo.groups b
        on a.group_id = b.group_id
    where 
        a.user_id = %s and a.parent_id = %s
    """
    tasks = {}
    tasks[GROUP_USERS] = []
    rows = db.get_records(sql, parameters=[uid, gid])
    for row in rows:
        model = {
            'group_id':row[0],
            'parent_id':row[1],
            'name':row[2],
            'culture':row[3],
            'group_code':row[4],
            'sequence':row[5],
            'is_childs':row[6],
            'depth':row[7],
            'is_display': row[8],
            'email':row[9],
            'remark':row[10],
            'created_date':str(row[11]),
            'modified_date':str(row[12]),
            'relation_type':row[13],
            'is_master':row[14]
        }
        tasks[GROUP_USERS].append(model)

    context['ti'].xcom_push(key=GROUPS, value=tasks[GROUP_USERS])
    return list(tasks.values())

# 결재할 정보 반영
def set_sign_activity(instance_id, contents, context):
    db = MySqlHook(mysql_conn_id='mariadb', schema="dapp")
    sql = f"""
    update sign_activity
        set contents = %s
    where 
        instance_id = %s
    """
    db.run(sql, autocommit=True, parameters=[contents, instance_id])

# 설정 마스터
def get_settings(**context):
    codes = get_codes()

    instance = context['ti'].xcom_pull(task_ids=INSTANCE_TASK, key=INSTANCE)

    logging.info(f'get_settings instances {instance}')

# WF 상태
def branch_status(**context):
    workflow = context['ti'].xcom_pull(task_ids=WORKFLOW_START_TASK, key=WORKFLOWS)
    if workflow["state"] == STATUS_00:
        return STATUS_00
    else:
        return STATUS_01
# 다음 결재자 정보
def next_activity(lst, context):
    NEXT_ACTIVITY = 'next_activity'
    SEQ = 'sequence'
    SIGN_AREA_ID = 'sign_area_id'
    tasks = {}
    tasks[NEXT_ACTIVITY] = []
    for m in lst:
        # 현결재자
        if m[SIGN_ACTION] == STATUS_01:
            current = m
            logging.info(f'현 결재자: {m}')
        
        # 미결재자
        elif m[SIGN_ACTION] == STATUS_02: 
            # 일반결재
            if m[SIGN_AREA] == STATUS_01: 
                # 다음 결재자의 결재영역이 같은가?
                if current[SIGN_AREA] == m[SIGN_AREA]:
                    if current[SEQ] + 1 == m[SEQ]:
                        # 일반결재
                        if m[SIGN_SECTION] == STATUS_01: 
                            # 일반결재
                            if m[SIGN_POSITION] == STATUS_01: 
                                tasks[NEXT_ACTIVITY].append(m)
                        # 병렬합의
                        elif m[SIGN_SECTION] == STATUS_02: 
                            tasks[NEXT_ACTIVITY].append(m)
                else:
                    if current[SIGN_AREA_ID] + 1 == m[SIGN_AREA_ID]:
                        # 일반결재
                        if m[SIGN_SECTION] == STATUS_01: 
                            # 일반결재 and 첫번째 결재
                            if m[SIGN_POSITION] == STATUS_01 and m[SEQ] == 1: 
                                tasks[NEXT_ACTIVITY].append(m)
                        # 병렬합의
                        elif m[SIGN_SECTION] == STATUS_02: 
                            tasks[NEXT_ACTIVITY].append(m)

            # elif m[SIGN_AREA] == STATUS_02: # 수신결재

    return list(tasks.values())
# 기안
def get_status_00(**context):
    try:
        workflow = context['ti'].xcom_pull(task_ids=WORKFLOW_START_TASK, key=WORKFLOWS)
        instance_id = int(workflow["instance_id"])    
        sign_activity = get_sign_activity(instance_id, context)
        sql = ''
        db = MySqlHook(mysql_conn_id='mariadb', schema="dapp")
        for row in sign_activity[0]:
            uid = row["user_id"]
            gid = row["group_id"]
            # 기안결재 && 현결재
            if row[SIGN_SECTION] == STATUS_00 and row[SIGN_ACTION] == STATUS_01:
                logging.info(f'기안: {row}')
                group = get_groups(gid, context)
                user = get_users(uid, context)
                # user['instance_id'] = instance_id

                # 다음 결재자 처리
                next_list = next_activity(sign_activity, context)
                logging.info(f'next list : {next_list}')
                # activity_list = []
                # activity_list.append(row)
                # sql += insert_signers_sql(activity_list, group, context)

                # user_list = []
                # user_list.append(user)            
                # sql += insert_sign_users_sql(user_list, row, context)

                # logging.info(f'user sql : {sql}')
            # else:
            #     logging.info(f'미결재: {row}')
        # conn = db.get_conn()
        # cursor = conn.cursor()
        # cursor.execute(sql)
        # cursor.close()         
        # conn.commit()
    except Exception as ex:
        set_error(workflow['workflow_process_id'], ex)
        logging.error(f'Message: {ex}')
    # contents = sign_activity["contents"]    
    # root = ET.fromstring(contents)
    # for doc in root.findall('signers'):
    #     # doc.find("group_id").text = "annguk"
    #     # ET.canonicalize(contents)
    #     uid = doc.find("user_id").text
    #     gid = doc.find("group_id").text
    #     if doc.attrib[SIGN_POSITION] == STATUS_00:
    #         logging.info(f'기안: {uid}')
    #         # 기안정보 등록
    #         group = get_groups(gid, context)
    #         set_signers(doc, group, context)
    #         root.remove(doc)
    #     else:
    #         logging.info(f'결재')
    #     logging.info(f'signers : {doc.find("group_id").text}, {doc.attrib}')

    # logging.info(f'modified: {ET.tostring(root)}')
    logging.info(f'get_status_00 : instance_id - {instance_id} , sign_activity - {sign_activity}')

    # set_sign_activity(instance_id, ET.tostring(root), context)
    return "annguk"

with models.DAG("workflow", default_args=default_args, schedule_interval=timedelta(minutes=1)) as dag:
    # Watch workflow process
    wf_sensor = WorkflowSensor(task_id='wf_sensor_task', poke_interval=3, dag=dag)
    # Start workflow    
    wf_start = PythonOperator(task_id=WORKFLOW_START_TASK, python_callable=get_workflow, provide_context=True, dag=dag)
    # Status get
    wf_status = BashOperator(task_id='wf_status_task',bash_command='echo wf_status get',dag=dag)
    # End workflow    
    wf_end = BashOperator(task_id='wf_end_task',bash_command='echo wf_end ',dag=dag)

    # instances
    instances = PythonOperator(task_id=INSTANCE_TASK,python_callable=get_instance, provide_context=True, dag=dag)

    # Settings
    settings = PythonOperator(task_id='settings_task',python_callable=get_settings, provide_context=True, dag=dag)

    # Status: 상태
    status = PythonOperator(task_id='status_task',python_callable=branch_status, provide_context=True, dag=dag)
    # 00: 기안
    status_00 = PythonOperator(task_id='status_00_task',python_callable=get_status_00, provide_context=True, dag=dag)
    # 01: 현결재 true || false
    status_01 = BashOperator(task_id='status_01_task',bash_command='echo get status 현결재 true || false',dag=dag)

    # Area: 결재영역
    area = BashOperator(task_id='area_task', bash_command='echo get area', dag=dag)
    # 00: 기안회수
    area_00 = BashOperator(task_id='area_00_task', bash_command='echo get area_00 기안회수', dag=dag)    
    # 01: 일반결재
    area_01 = BashOperator(task_id='area_01_task', bash_command='echo get area_01 일반결재', dag=dag)
    # 02: 수신결재
    area_02 = BashOperator(task_id='area_02_task', bash_command='echo get area_02 수신결재', dag=dag)
    # 03: 부서합의
    area_03 = BashOperator(task_id='area_03_task', bash_command='echo get area_03 부서합의', dag=dag)

    # Section: 결재구분
    section = BashOperator(task_id='section_task', bash_command='echo get section 결재구분', dag=dag)
    # 00: 기안결재
    section_00 = BashOperator(task_id='section_00_task', bash_command='echo get section_00 기안결재', dag=dag)
    # 01: 일반결재
    section_01 = BashOperator(task_id='section_01_task', bash_command='echo get section_01 일반결재', dag=dag)
    # 02: 병렬합의
    section_02 = BashOperator(task_id='section_02_task', bash_command='echo get section_02 병렬합의', dag=dag)
    # 03: 순차합의
    section_03 = BashOperator(task_id='section_03_task', bash_command='echo get section_03 순차합의', dag=dag)

    # Process: 결재 프로세스 처리(결재선 & 결재함)
    process = BashOperator(task_id='process_task', bash_command='echo get process 결재 프로세스 처리(결재선 & 결재함)', dag=dag)

    # Event: 결재 이벤트 처리
    event = BashOperator(task_id='event_task', bash_command='echo get event 결재 이벤트 처리', dag=dag)
    # 00: 진행중
    event_00 = BashOperator(task_id='event_00_task', bash_command='echo get event_00 진행중 이벤트 처리', dag=dag)
    # 01: 완료
    event_01 = BashOperator(task_id='event_01_task', bash_command='echo get event_01 완료 이벤트 처리', dag=dag)

    # Notify: 알림 이벤트 처리
    notify = BashOperator(task_id='notify_task', bash_command='echo get notify 알림 이벤트 처리', dag=dag)
    # 00: 이메일
    notify_00 = BashOperator(task_id='notify_00_task', bash_command='echo get notify_00 이메일 알림 처리', dag=dag)
    # 01: 쪽지
    notify_01 = BashOperator(task_id='notify_01_task', bash_command='echo get notify_01 쪽지 알림 처리', dag=dag)
    # 02: 모바일
    notify_02 = BashOperator(task_id='notify_02_task', bash_command='echo get notify_02 모바일 알림 처리', dag=dag)

    # Doc: 문서발번
    doc = BashOperator(task_id='doc_task', bash_command='echo get doc 문서발번 처리', dag=dag)

    # Auth: 권한처리
    # auth = BashOperator(task_id='auth_task', bash_command='echo get auth 권한 처리', dag=dag)

    # Complete: 완료처리
    complete = BashOperator(task_id='complete_task', bash_command='echo get complete 완료처리', dag=dag)

    # Workflow Start
    wf_sensor >> wf_start >> instances >> settings >> status
    # 결재중이 아니면 완료 처리
    instances >> complete
    
    # 결재상태
    status >> status_00 >> section
    status >> status_01

    # 결재영역 >> 결재구분 >> 결재 프로세스 >> 결재 이벤트
    status_01 >> area >> [area_00, area_01, area_02, area_03] >> section >> [section_00, section_01, section_02, section_03] >> process >> event

    # 결재 이벤트 >> 대상자 알림 >> 결재 엔진 상태 종료
    event >> [event_00, event_01] >> complete >> doc >> notify >> [notify_00, notify_01, notify_02] >> wf_end