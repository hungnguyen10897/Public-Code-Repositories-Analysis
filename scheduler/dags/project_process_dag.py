from typing_extensions import OrderedDict
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

from ...utils import PROJECT_PATH
# from ...utils import AIRFLOW_CONFIG
AIRFLOW_CONFIG = OrderedDict([('start_date', '2021-07-22'), ('email_on_failure', 'False'), ('email', ''), ('platform_dag_interval', '0 0 */3 * *'), ('project_process_dag_interval', '0 0 * * 0')])

default_args = {
    'owner': 'hung',
    'depends_on_past': False,
    'start_date': datetime.strptime(AIRFLOW_CONFIG["start_date"], "%Y-%m-%d"),
    'email': [AIRFLOW_CONFIG["email"]],
    'email_on_failure': bool(AIRFLOW_CONFIG["email_on_failure"]),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('project_processing', default_args = default_args, schedule_interval = AIRFLOW_CONFIG["project_process_dag_interval"])

t1 = BashOperator(
    task_id = "project_processing",
    dag = dag,
    bash_command = f"cd {PROJECT_PATH}/spark && spark-submit --driver-class-path postgresql-42.2.12.jar spark_project.py"
)

t1
