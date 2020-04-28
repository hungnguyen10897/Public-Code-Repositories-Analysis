from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

import sys
repo_dir = "/home/hung/MyWorksapce/BachelorThesis/repo"
sys.path.append(repo_dir)

from jenkins_data.fetch_jenkins_data import fetch_jenkins
from sonarcloud_data.fetch_sonarcloud_data import fetch_sonar_data
from merge_stage_archive import main

from datetime import datetime, timedelta

default_args = {
    'owner': 'hung',
    'depends_on_past': False,
    'start_date': datetime(2020, 4, 8),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('platform', default_args = default_args, schedule_interval = timedelta(days= 1))

t1_jenkins = PythonOperator(
    task_id = 'fetch_jenkins_data',
    provide_context=False,
    python_callable= fetch_jenkins,
    op_args=[True, None, f'{repo_dir}/jenkins_data/data', True],
    dag = dag
)

t1_sonar = PythonOperator(
    task_id = 'fetch_sonarqube_data',
    provide_context=False,
    python_callable= fetch_sonar_data,
    op_args=[f'{repo_dir}/sonarcloud_data/data'],
    dag = dag
)

t2 = BashOperator(
    task_id = "spark_processing",
    dag = dag,
    bash_command = f"cd {repo_dir} && spark-submit --driver-class-path postgresql-42.2.12.jar spark.py"
)

t3 = PythonOperator(
    task_id = "merge_stage_archive",
    provide_context=False,
    python_callable= main,
    op_args=[f"{repo_dir}/jenkins_data/data", f"{repo_dir}/sonarcloud_data/data"],
    dag = dag
)

t1_jenkins >> t2
t1_sonar >> t2
t2 >> t3

