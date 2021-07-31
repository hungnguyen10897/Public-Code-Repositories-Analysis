# Installation Guide

This guide provides necessary steps and information to install the platform. Each step sets up one component, which has internal dependency on other previous components. Therefore, the steps should be carried out in correct order.

**Note**: All commands are to be run in the root directory of the repository

## Prerequisites
Some tools that should be pre-installed before starting the guide:
    - Docker
    - Docker-compose
    - Apache Superset
    
## Step 1: Git and submodule initialization

Clone the repo at: https://github.com/hungnguyen10897/Public-Code-Repositories-Analysis

There are/will be some parts of this project, e.g the sonarqube extractor, which are developed as separate projects and included as a [git submodule](https://git-scm.com/book/en/v2/Git-Tools-Submodules). Therefore, after cloning the project repository, we need to initialize all of these submodules:

```
$ git submodule update --init --recursive
```

## Step 2: Environment Varialbes

Export an environment varialbe pointing to the root directory of the repository just cloned in step 1
```
$ export PRA_HOME=/mnt/pra
```
## Step 3: Backend Database

Start Postgres Backend Database with `docker-compose`. At `PRA_HOME` directory, go to `db` and run
```
$ docker-compose up -d
```
This creates a container running Postgres, with `airflow`, `superset` and `pra` databases and respective users for each of them. `airflow` and `superset` databases will be used and configured later by Apache Airflow and Apache Superset later. `pra` database is where data of the platform is stored in relevant tables.

User, Database creation and schema definitions for `pra` tables are all defined in `db/init-user-db.sh`.

Postgres root user can be changed in `db/postgres-dockerfile` by setting `POSTGRES_USER`, `POSTGRES_PASSWORD` and `POSTGRES_DB`

A new directory `db/db_home` is created and mounted to the container, containing underlying data for persistence.
## Step 4: Python Environment

The tool is develoepd and run on Python 3.6.9

To install all necessary packages: 
```
$ python -m pip install -U pip
$ pip install -r requirements.txt
```
## Step 5: Airflow Pipelines

### 5a: Installing Apache Airflow
**Note**: You can skip this part if you use your own Airflow installation, this guide uses Apache Airflow 1.10.11

Make sure you are using Python 3 and corresponding `pip`

```
$ export AIRFLOW_HOME=[YOUR_AIRFLOW_HOME]
$ python -m pip install -U pip
$ pip install apache-airflow==1.10.11
```

Change some Airflow configurations, in file `$AIRFLOW_HOME/airflow.cfg` find and change the following configurations:

```
...
sql_alchemy_conn = postgresql+psycopg2://airflow:airflow@localhost:5432
...
load_examples = False
...
```

### 5b: Installing and Configuring Airflow DAGs

Change `config.cfg` file, espescially the `start_date` config under `[AIRFLOW]`, it should be the date of installation. There should be some more configurations if you want `email_on_failure`, details [here](https://helptechcommunity.wordpress.com/2020/04/04/airflow-email-configuration/). The `_interval` values specify how often the DAGs got triggered, they have cron-job syntax.

After installing airflow, an environment variable `AIRFLOW_HOME` is exported, poiting to the home directory of Airflow. Then we need to copy DAG files (`.py` files) under `scheduler/dags` to `AIRFLOW_HOME/dags` directory

at `PRA_HOME`:
```
$ cp scheduler/dags/platform_dag.py $AIRFLOW_HOME/dags/platform_dag.py
$ cp scheduler/dags/project_process_dag.py $AIRFLOW_HOME/dags/project_process_dag.py
```

Initialize Airflow:
```
$ airflow initdb
$ airflow scheduler -D
```

To stop Airflow Scheduler
```
$ cat $AIRFLOW_HOME/airflow-scheduler.pid | xargs kill
```

Verify and Start DAGs:
```
$ airflow list_dags
$ airflow unpause platform
```

To start Airflow Webserver (Optional):
```
$ airflow webserver
```

## Step 6: Superset UI
This part assumes you have Apache Superset installed. 

First, go to backups/datasources.yaml and change `sqlalchemy_uri` entry. It should have the following structure

```
postgresql+psycopg2://[USERNAME]:[PASSWORD]@[HOST_ADDRESS]:[PORT]/[DATABASE]
```
For example
```
postgresql+psycopg2://pra:pra@135.232.51.219:5432/pra
```

Then import datasources and dashboards, the commands need to be executed in this exact order

```
superset import_datasources -p backups/datasources.yaml
superset import-dashboards -p backups/dashboards.json
```