"""
Example usage of the TriggerDagRunOperator. This example holds 2 DAGs:
1. 1st DAG (example_trigger_controller_dag) holds a TriggerDagRunOperator, which will trigger the 2nd DAG
2. 2nd DAG (example_trigger_target_dag) which will be triggered by the TriggerDagRunOperator in the 1st DAG
"""
from airflow import DAG
from airflow import models
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

dag = DAG(
    dag_id="example_trigger_target_dag",
    default_args={"owner": "airflow"},
    start_date=days_ago(2),
    schedule_interval=None,
    tags=['example']
)


def run_this_func(**context):
    """
    Print the payload "message" passed to the DagRun conf attribute.
    :param context: The execution context
    :type context: dict
    """
    print("Remotely received value of {} for key=message".format(context["dag_run"].conf["message"]))


# run_this = PythonOperator(task_id="run_this", python_callable=run_this_func, dag=dag)

bash_task = BashOperator(
    task_id="bash_task",
    bash_command='echo "여기에 메시지 보여주기: $message"',
    env={'message': '{{ dag_run.conf["message"] if dag_run else "" }}'},
    dag=dag,
)