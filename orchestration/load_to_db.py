import sys, os
if "PRA_HOME" not in os.environ:
    print("Please set environment variable PRA_HOME before running.")
    sys.exit(1)

project_path = os.environ['PRA_HOME']

import pandas as pd
from pathlib import Path
import sys, argparse
from sqlalchemy import create_engine
from utils import *

def load(file_directory, conn_str):

    print("Start writing CSV files to Database:")
    dirs, dtype_dicts = iter_data_directory(data_dir)

    for file_directory, dtype in zip(dirs, dtype_dicts):
        if not file_directory.exists():
            return

        if dtype == JENKINS_BUILD_DTYPE:
            table_name = "jenkins_builds"
        elif dtype == JENKINS_TEST_DTYPE:
            table_name = "jenkins_tests"
        elif dtype == SONAR_ANALYSES_DTYPE:
            table_name = "sonar_analyses"
        elif dtype == SONAR_ISSUES_DTYPE:
            table_name = "sonar_issues"
        elif dtype == SONAR_MEASURES_DTYPE:
            table_name = "sonar_measures"
            
        print(f"\tDirectory: {file_directory.absolute()} - Table: {table_name}")

        for file in file_directory.glob("*_staging.csv"):
            if file.exists():

                new_df = pd.read_csv(file.resolve(), dtype = dtype, header = 0)
                # load to DB

                engine = create_engine(conn_str)
                new_df.to_sql(table_name, engine, if_exists='append', index=False)

if __name__ == "__main__":
    
    ap = argparse.ArgumentParser(description="Script to merge staging and archive files.")
    ap.add_argument("-d","--data-directory", default=f'{project_path}/data' , help="Path to data directory.")
    ap.add_argument("-c","--connection-string", default=f'postgresql+psycopg2://pra:pra@localhost/pra2' , help="Connection string to Database.")

    args = vars(ap.parse_args())
    data_dir = args['data_directory']
    conn_str = args['connection_string']
    load(data_dir, conn_str)