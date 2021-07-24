import pandas as pd
import os, sys, argparse
from sqlalchemy import create_engine

assert "PRA_HOME" in os.environ
assert os.environ["PRA_HOME"] in sys.path

from scheduler.workflow_tasks.workflow_utils import *
from utils import PRA_HOME

def load(data_dir=f'{PRA_HOME}/data', conn_str=CONNECTION_STR, all=False):

    print("Start writing CSV files to Database:")
    dirs, dtype_dicts = iter_data_directory(data_dir)

    for file_directory, dtype in zip(dirs, dtype_dicts):
        if not file_directory.exists():
            continue

        if dtype == JENKINS_BUILD_DTYPE:
            table_name = "jenkins_builds"
        elif dtype == SONAR_ANALYSES_DTYPE:
            table_name = "sonar_analyses"
        elif dtype == SONAR_ISSUES_DTYPE:
            table_name = "sonar_issues"
        elif dtype == SONAR_MEASURES_DTYPE:
            table_name = "sonar_measures"
            
        print(f"\tDirectory: {file_directory.absolute()} - Table: {table_name}")

        if all:
            file_filter = "*.csv"
        else:
            file_filter = "*_staging.csv"

        for file in file_directory.glob(file_filter):
            if file.exists():

                new_df = pd.read_csv(file.resolve(), usecols = list(dtype.keys()),dtype = dtype, header = 0)
                # load to DB

                engine = create_engine(conn_str)
                new_df.to_sql(table_name, engine, if_exists='append', index=False)

if __name__ == "__main__":
    
    ap = argparse.ArgumentParser(description="Script to load CSV files to Database.")
    ap.add_argument("-d","--data-directory", default=f'{PRA_HOME}/data' , help="Path to data directory.")
    ap.add_argument("-a","--all", dest="all" , action="store_true", default=False , help="Whether to load all files including Staging and Archive.")

    args = vars(ap.parse_args())
    data_dir = args['data_directory']
    all = args['all']
    load(data_dir, all=all)