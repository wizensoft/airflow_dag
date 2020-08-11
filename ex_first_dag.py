from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mysql_operator import MySqlOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'todd',
    'depends_on_past': False,
    'start_date': datetime(2015, 6, 1),
    'email': ['todd@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(dag_id='ex_first_dag',
          default_args=default_args,
          schedule_interval=timedelta(days=1))

# Task 1
t1 = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag)


# Task 2
def my_python_function():
    now = datetime.now()
    response = 'This function ran at ' + str(now)
    return response


t2 = PythonOperator(
    task_id='my_python_task',
    python_callable=my_python_function,
    params={'my_param': 'Parameter I passed in'},
    dag=dag)


# Task 3
t3 = MySqlOperator(task_id='mariadb_task',
                      sql="INSERT INTO test VALUES (1, '이찬호', 'my first dag');",
                      mysql_conn_id='mariadb',
                      autocommit=True,
                      database="djob",
                      dag=dag)

# Pipeline Structure
t2.set_upstream(t1)
t3.set_upstream(t2)