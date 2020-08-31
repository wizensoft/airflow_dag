import json
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.operators.subdag_operator import SubDagOperator
from wizen_plugin.subdag.t_subdag import t_subdag

def load_dag(**kwargs):
    number_of_runs = json.dumps(kwargs['dag_run'].conf['number_of_runs'])
    dag_data = json.dumps({
        "number_of_runs": number_of_runs
    })
    return dag_data

default_args = {
    "owner": "mk",
    "depends_on_past": False,
    "start_date": datetime(2019, 7, 1),
    "retries": 1,
}
dag = DAG("t_maindag"
        ,default_args=default_args
        ,schedule_interval="@once")

load_config = PythonOperator(
    task_id='load_config',
    provide_context=True,
    python_callable=load_dag,
    dag=dag)
# t1, t2 and t3 are examples of tasks created by instantiating operators
t1 = BashOperator(task_id="print_date", bash_command="date", dag=dag)
t2 = BashOperator(task_id="sleep", bash_command="sleep 3", retries=3, dag=dag)

templated_command = """

    {% for i in range(5) %}
        echo "{{ ds }}"
        echo "{{ macros.ds_add(ds, 7)}}"
        echo "{{ params.my_param }}"
    {% endfor %}

"""

t3 = BashOperator(
    task_id="templated",
    bash_command=templated_command,
    params={"my_param": "Parameter I passed in"},
    dag=dag,
)

t4 = SubDagOperator( # 여기서 아까 만든 애를 불러옵니다
    task_id='sub_dag',
    subdag=t_subdag(dag.dag_id, 'sub_dag', default_args),
    default_args=default_args,
    dag=dag,
)

t2.set_upstream(t1)
t3.set_upstream(t1)

t3 >> t4