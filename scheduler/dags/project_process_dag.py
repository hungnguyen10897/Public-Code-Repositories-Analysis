from typing_extensions import OrderedDict
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

# Has to be done this way since this file will be located at: AIRFLOW_HOME/dags directory
import os, sys
if "PRA_HOME" not in os.environ:
    print("Please set environment variable PRA_HOME before running.")
    sys.exit(1)

sys.path.insert(1, os.environ["PRA_HOME"])
from utils import PRA_HOME, AIRFLOW_CONFIG

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
    bash_command = f"cd {PRA_HOME}/spark && spark-submit --driver-class-path postgresql-42.2.12.jar spark_project.py"
)

t1
