import sys
import logging
import unittest
from datetime import datetime
from airflow.models import DagBag
from airflow.models import TaskInstance

class TestDagIntegrity(unittest.TestCase):

    LOAD_SECOND_THRESHOLD = 2

    def setUp(self):
        self.dagbag = DagBag()

    def test_settings(self):
        """Check task of get_settings dag"""
        dag_id='workflow'                
        dag = self.dagbag.get_dag(dag_id)
        wf_start_task = dag.get_task('wf_start_task')
        instances_task = dag.get_task('instances_task')
        settings_task = dag.get_task('settings_task')

        execution_date = datetime.now()

        wf_start_task_ti = TaskInstance(task=wf_start_task, execution_date=execution_date)
        wf_start_task_context = wf_start_task_ti.get_template_context()
        wf_start_task.execute(wf_start_task_context)

        # # workflow_result = wf_start_task_ti.xcom_pull(key="workflow")
        # # logging.info(f'workflow_result : {workflow_result}')

        # instances_task_ti = TaskInstance(task=instances_task, execution_date=execution_date)
        # instances_task_context = instances_task_ti.get_template_context()
        # instances_task.execute(instances_task_context)

        # # instance_result = instances_task_ti.xcom_pull(key="instance")
        # # logging.info(f'instance_result : {instance_result}')

        # settings_task_ti = TaskInstance(task=settings_task, execution_date=execution_date)
        # settings_task_context = settings_task_ti.get_template_context()
        # settings_task.execute(settings_task_context)

        # settings_result = settings_task_ti.xcom_pull(key="instance")
        # logging.info(f'settings_result : {settings_result}')        
        
        # self.assertGreater(result.__len__ > 0)

# suite = unittest.TestLoader().loadTestsFromTestCase(TestDagIntegrity)
# unittest.TextTestRunner(verbosity=2).run(suite)