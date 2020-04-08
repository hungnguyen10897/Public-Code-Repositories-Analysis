from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

import sys
sys.path.append("/home/hung/MyWorksapce/BachelorThesis/repo")

from jenkins_data.fetch_jenkins_data import fetch_jenkins
from sonarcloud_data.fetch_sonarcloud_data import fetch_sonarqube

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
    op_args=[True, None, './jenkins_data/data', True],
    dag = dag
)

t1_sonar = PythonOperator(
    task_id = 'fetch_sonarqube_data',
    provide_context=False,
    python_callable= fetch_sonarqube,
    op_args=['csv', './sonarcloud_data/sonar_data'],
    dag = dag
)

