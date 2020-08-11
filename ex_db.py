"""
 - This is an idea for how to invoke multiple tasks based on the query results
"""
import logging
from datetime import datetime

from airflow import DAG
from airflow.hooks.mysql_hook import MySqlHook
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator

from airflow.operators.dummy_operator import DummyOperator
# from airflow.operators import GetWorkflowOperator
# from include.run_celery_task import runCeleryTask

########################################################################

default_args = {
    'owner': 'airflow',
    'catchup': False,
    'depends_on_past': False,
    'start_date': datetime(2019, 7, 2, 19, 50, 00),
    'email': ['rotten@stackoverflow'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'max_active_runs': 1
}

dag = DAG('ex_db', default_args=default_args, schedule_interval=None)

###########################################################################################################

# Generate a set of tasks so we can parallelize the results
def createOrderProcessingTask(bucket_number):
    return PythonOperator( 
                           task_id=f'order_processing_task_{bucket_number}',
                           python_callable=runOrderProcessing,
                           pool='order_processing_pool',
                           op_kwargs={'task_bucket': f'order_processing_task_{bucket_number}'},
                           provide_context=True,
                           dag=dag
                          )


# Fetch the order arguments from xcom and doStuff() to them
def runOrderProcessing(task_bucket, **context):
    items = context['ti'].xcom_pull(task_ids='get_open_orders', key=task_bucket)

    if items is not None:
        for order in items:
            logging.info(f"Processing Order with Order ID {order[order_id]}, customer ID {order[customer_id]}")
            doStuff(**op_kwargs)


# Discover the orders we need to run and group them into buckets for processing
def getWorkflows(**context):
    db = MySqlHook(mysql_conn_id='mariadb', schema="djob")
    # initialize the task list buckets
    sql = """
    select 
        o.id,
        o.name,
        o.desc
    from 
        test o
    """    
    tasks = {}
    rowCount = 0    
    rows = db.get_records(sql)
    for row in rows:        
        rowCount += 1
        tasks[f'order_processing_task_{rowCount}'] = []        

    # populate the task list buckets
    # distribute them evenly across the set of buckets
    # records: List[List[Optional[Any]]] = db.get_records(get_orders_query)

    resultCount = 0
    for row in rows:
        resultCount += 1
        model = {'id': str(row[0]), 'name': str(row[1])}
        # tasks[f'order_processing_task_{bucket}'] = []
        tasks[f'order_processing_task_{resultCount}'].append(model)

    items = {}
    # Push the order lists into xcom
    for task in tasks:
        if len(tasks[task]) > 0:
            logging.info(f'Task {task} has {len(tasks[task])} orders.')
            context['ti'].xcom_push(key=task, value=tasks[task])
                        
    return list(tasks.values())

dummy_task = DummyOperator(task_id='dummy_task', dag=dag)

# operator_task = MyFirstOperator(my_operator_param='This is a test.',
#                                 task_id='my_first_operator_task', dag=dag)

# dummy_task >> operator_task    

###################################################################################################


# this just makes sure that there aren't any dangling xcom values in the database from a crashed dag
# clean_xcoms = MySqlOperator(
#     task_id='clean_xcoms',
#     mysql_conn_id='airflow_db',
#     sql="delete from xcom where dag_id='{{ dag.dag_id }}'",
#     dag=dag)


# Ideally we'd use BranchPythonOperator() here instead of PythonOperator so that if our
# query returns fewer results than we have buckets, we don't try to run them all.
# Unfortunately I couldn't get BranchPythonOperator to take a list of results like the
# documentation says it should (Airflow 1.10.2). So we call all the bucket tasks for now.
get_orders_task = PythonOperator(
                                 task_id='get_workflows',
                                 python_callable=getWorkflows,
                                 provide_context=True,
                                 dag=dag
                                )
# open_order_task.set_upstream(clean_xcoms)

# set up the parallel tasks -- these are configured at compile time, not at run time:
for bucketNumber in range(1, 5):
    taskBucket = createOrderProcessingTask(bucketNumber)
    taskBucket.set_upstream(get_orders_task)
