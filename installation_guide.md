# Installation Guide

This guide provides necessary steps and information to install the platform. Each step sets up one component, which has internal dependency on other previous components. Therefore, the steps should be carried out in correct order.

**Note**: All commands are to be run in the root directory of the repository

## Prerequisites
Some tools that should be pre-installed before starting the guide:
    - Docker
    - Docker-compose
    
## Step 1: Git and submodule initialization

Clone the repo at: https://github.com/hungnguyen10897/Public-Code-Repositories-Analysis

There are/will be some parts of this project, e.g the sonarqube extractor, which are developed as separate projects and included as a [git submodule](https://git-scm.com/book/en/v2/Git-Tools-Submodules). Therefore, after cloning the project repository, we need to initialize all of these submodules:

```
git submodule update --init --recursive
```

## Step 2: Environment Varialbes

Export an environment varialbe pointing to the root directory of the repository just cloned in step 1
```
export PRA_HOME=/mnt/pra
```
## Step 3: Backend Database

Start Postgres Backend Database with `docker-compose`. At `PRA_HOME` directory, go to `db` and run
```
docker-compose up -d
```
This creates a container running Postgres, with `airflow`, `superset` and `pra` databases and respective users for each of them. `airflow` and `superset` databases will be used and configured later by Apache Airflow and Apache Superset later. `pra` database is where data of the platform is stored in relevant tables.

User, Database creation and schema definitions for `pra` tables are all defined in `db/init-user-db.sh`.

Postgres root user can be changed in `db/postgres-dockerfile` by setting `POSTGRES_USER`, `POSTGRES_PASSWORD` and `POSTGRES_DB`

A new directory `db/db_home` is created and mounted to the container, containing underlying data for persistence.
## Step 4: Python Environment

The tool is develoepd and run on Python 3.6.9

To install all necessary packages: 
```
pip install -r requirements.txt
```
## Step 5: Airflow Pipelines

## Step 6: Superset UI