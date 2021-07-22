from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

from ...utils import PROJECT_PATH
# from ...utils import AIRFLOW_CONFIG
from typing_extensions import OrderedDict
AIRFLOW_CONFIG = OrderedDict([('start_date', '2021-07-22'), ('email_on_failure', 'False'), ('email', ''), ('platform_dag_interval', '0 0 */3 * *'), ('project_process_dag_interval', '0 0 * * 0')])
from ...scheduler.workflow_tasks import fetch_data, load_to_db, merge_stage_archive, stamp

default_args = {
    'owner': 'hung',
    'depends_on_past': False,
    'start_date': datetime.strptime(AIRFLOW_CONFIG["start_date"], "%Y-%m-%d"),
    'email': [AIRFLOW_CONFIG["email"]],
    'email_on_failure': bool(AIRFLOW_CONFIG["email_on_failure"]),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('platform', default_args = default_args, schedule_interval = AIRFLOW_CONFIG["platform_dag_interval"])

t1_jenkins = PythonOperator(
    task_id = 'fetch_jenkins_data',
    provide_context=False,
    python_callable= fetch_data.run,
    op_args=["jenkins"],
    dag = dag
)

t1_sonar = PythonOperator(
    task_id = 'fetch_sonarqube_data',
    provide_context=False,
    python_callable= fetch_data.run,
    op_args=["sonarcloud"],
    dag = dag
)

t2 = PythonOperator(
    task_id = 'load_data_to_db',
    provide_context=False,
    python_callable= load_to_db.load,
    # op_args=[],
    dag = dag
)

t3 = BashOperator(
    task_id = "spark_processing",
    dag = dag,
    bash_command = f"cd {PROJECT_PATH}/spark && spark-submit --driver-class-path postgresql-42.2.12.jar spark.py"
)

t4_merge = PythonOperator(
    task_id = 'merge_stage_archive',
    provide_context=False,
    python_callable= merge_stage_archive.merge,
    # op_args=[f"{PROJECT_PATH}/data"],
    dag = dag
)

t4_stamp = PythonOperator(
    task_id = 'stamp',
    provide_context=False,
    python_callable= stamp.run,
    # op_args=[],
    dag = dag
)

t1_jenkins >> t2
t1_sonar >> t2
t2 >> t3
t3 >> t4_merge
t3 >> t4_stamp
