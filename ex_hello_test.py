import sys
import unittest
from datetime import datetime
from airflow.models import DagBag
from airflow.models import TaskInstance

class TestDagIntegrity(unittest.TestCase):

    LOAD_SECOND_THRESHOLD = 2

    def setUp(self):
        self.dagbag = DagBag()

    def test_task_count(self):
        """Check task count of ex_hello dag"""
        dag_id='ex_hello'        
        dag = self.dagbag.get_dag(dag_id)
        self.assertEqual(len(dag.tasks), 2)

    def test_contain_tasks(self):
        """Check task contains in ex_hello dag"""
        dag_id='ex_hello'        
        dag = self.dagbag.get_dag(dag_id)
        tasks = dag.tasks
        task_ids = list(map(lambda task: task.task_id, tasks))
        self.assertListEqual(task_ids, ['dummy_task', 'hello_task'])        

    def test_task_hello(self):
        """Check task count of ex_hello dag"""
        dag_id='ex_hello'                
        dag = self.dagbag.get_dag(dag_id)
        python_task = dag.get_task('hello_task')
        
        downstream_task_ids = list(map(lambda task: task.task_id, python_task.upstream_list)) 
        self.assertEqual(len(downstream_task_ids), 1)

        ti = TaskInstance(task=python_task, execution_date=datetime.now())
        result = python_task.execute(ti.get_template_context())
        self.assertEqual(result, 'Hello Wolrd')

suite = unittest.TestLoader().loadTestsFromTestCase(TestDagIntegrity)
unittest.TextTestRunner(verbosity=2).run(suite)