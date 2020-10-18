# pyspark --driver-class-path postgresql-42.2.12.jar
# spark-submit --driver-class-path postgresql-42.2.12.jar spark.py

from pyspark import SparkContext, SparkConf
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import Row
from pyspark.sql.utils import AnalysisException
from pyspark.sql.functions import udf
from pyspark.ml.feature import StringIndexer, StringIndexerModel, VectorAssembler, MinMaxScaler, Imputer, ChiSqSelector, ChiSqSelectorModel
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.classification import LogisticRegression, LogisticRegressionModel, DecisionTreeClassifier, DecisionTreeClassificationModel, RandomForestClassifier, RandomForestClassificationModel
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, BinaryClassificationEvaluator
from pyspark.ml.linalg import DenseVector, SparseVector
from pyspark.ml.stat import ChiSquareTest

import pandas as pd
import numpy as np
from pathlib import Path
from collections import OrderedDict
import time, sys, argparse

from spark_constants import *
from model_1 import apply_ml1
from model_2 import apply_ml2
from model_3 import apply_ml3
from common import train_predict, feature_selector_process, pipeline_process

conf = SparkConf().setMaster('local[*]')
sc = SparkContext
spark = SparkSession.builder.config(conf = conf).getOrCreate()

def get_data_from_file(source ,data_directory, load):

    file_extension = "/*.csv" if load == "first" else "/*_staging.csv"

    data_path = Path(data_directory)
    if source == "jenkins builds":
        files_dir = data_path.joinpath('builds')
        DTYPE = JENKINS_BUILD_DTYPE
    elif source == "jenkins tests":
        files_dir = data_path.joinpath('tests')
        DTYPE = JENKINS_TEST_DTYPE
    elif source == "sonar measures":
        files_dir = data_path.joinpath('measures')
        DTYPE = SONAR_MEASURES_DTYPE
    elif source == "sonar analyses":
        files_dir = data_path.joinpath('analyses')
        DTYPE = SONAR_ANALYSES_DTYPE
    elif source == "sonar issues":
        files_dir = data_path.joinpath('issues')
        DTYPE = SONAR_ISSUES_DTYPE            

    field = []
    for col,type in DTYPE.items():
        if col in ['date', 'last_commit_date', 'commit_ts', 'creation_date', 'update_date', 'close_date']:
            field.append(StructField(col, TimestampType(), True))
        elif type == 'object':
            field.append(StructField(col, StringType(), True))
        elif type == 'Int64':
            field.append(StructField(col, LongType(), True))
        elif type == 'float64':
            field.append(StructField(col, DoubleType(), True))

    schema = StructType(field)
    try:
        df = spark.read.csv(str(files_dir.absolute()) + file_extension, sep=',', schema = schema, ignoreLeadingWhiteSpace = True, 
            ignoreTrailingWhiteSpace = True, header=True, mode = 'FAILFAST')
    except AnalysisException:
        print(f"No .csv files for [{source}].")
        df = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)
    return df.drop_duplicates()

def run(jenkins_data_directory, sonar_data_directory, spark_artefacts_dir, run_mode):

    write_data = True
    if run_mode == 'update_models':
        run_mode = "first"
        write_data = False

    # Check for resources that enable incremental run
    if run_mode == "incremental":
        for i in ['1','2','3']:
            for suffix in ["", "_top_10"]:
                for obj in [f"pipeline_{i}",f"LogisticRegressionModel_{i}{suffix}",f"DecisionTreeModel_{i}{suffix}",f"RandomForestModel_{i}{suffix}", 
                        f"ChiSquareSelectorModel_{i}", "label_indexer_3"]:

                    obj_path = Path(spark_artefacts_dir).joinpath(obj)
                    if not obj_path.exists():
                        print(f"{obj} does not exist in spark_artefacts. Rerun with run_mode = first")
                        run(jenkins_data_directory, sonar_data_directory, spark_artefacts_dir, "first")
                        return

        # Data from db
        try:
            db_jenkins_builds = spark.read.jdbc(CONNECTION_STR, "jenkins_builds", properties=CONNECTION_PROPERTIES)
            db_sonar_analyses = spark.read.jdbc(CONNECTION_STR, "sonar_analyses", properties=CONNECTION_PROPERTIES) 
            db_sonar_measures = spark.read.jdbc(CONNECTION_STR, "sonar_measures", properties=CONNECTION_PROPERTIES) 
            db_sonar_issues = spark.read.jdbc(CONNECTION_STR, "sonar_issues", properties=CONNECTION_PROPERTIES) 

            for table,name in [(db_jenkins_builds,"jenkins_builds"), (db_sonar_analyses, "sonar_analyses"), (db_sonar_measures, "sonar_measures"), (db_sonar_issues, "sonar_issues")]:
                table.persist()
                if table.count() == 0:
                    print(f"No data in table [{name}]. Rerun with run_mode = first")
                    run(jenkins_data_directory, sonar_data_directory, spark_artefacts_dir, "first")
                    return
                    
        except Exception as e:
            print(f"Exception thrown when reading tables from Postgresql - {str(e)}. Rerun with run_mode = first")
            run(jenkins_data_directory, sonar_data_directory, spark_artefacts_dir, "first")
            return

    elif run_mode == "first":
        db_jenkins_builds = None
        db_sonar_analyses = None
        db_sonar_measures = None
        db_sonar_issues = None

    new_jenkins_builds = get_data_from_file("jenkins builds",jenkins_data_directory, run_mode)
    new_jenkins_builds = new_jenkins_builds.filter("job IS NOT NULL")
    new_jenkins_builds.persist()
    print("Jenkins builds Count: ", new_jenkins_builds.count())

    new_sonar_analyses = get_data_from_file("sonar analyses", sonar_data_directory, run_mode)
    new_sonar_analyses = new_sonar_analyses.filter("project IS NOT NULL AND analysis_key IS NOT NULL")
    new_sonar_analyses.persist()
    print("Sonar analyses Count: ", new_sonar_analyses.count())

    new_sonar_measures = get_data_from_file("sonar measures", sonar_data_directory, run_mode)
    new_sonar_measures = new_sonar_measures.filter("project IS NOT NULL AND analysis_key IS NOT NULL")
    new_sonar_measures = new_sonar_measures.drop(*TO_DROP_SONAR_MEASURES_COLUMNS)
    new_sonar_measures.persist()
    print("Sonar measures Count: ", new_sonar_measures.count())

    new_sonar_issues = get_data_from_file("sonar issues", sonar_data_directory, run_mode)
    new_sonar_issues = new_sonar_issues.filter("project IS NOT NULL AND issue_key IS NOT NULL")
    new_sonar_issues.persist()
    print("Sonar issues Count: ", new_sonar_issues.count())

    # UPDATE DB_ DF
    db_jenkins_builds = None if db_jenkins_builds is None else db_jenkins_builds.union(new_jenkins_builds)
    db_sonar_analyses = None if db_sonar_analyses is None else db_sonar_analyses.union(new_sonar_analyses)
    db_sonar_measures = None if db_sonar_measures is None else db_sonar_measures.union(new_sonar_measures)
    db_sonar_issues = None if db_sonar_issues is None else db_sonar_issues.union(new_sonar_issues)
    
    if write_data:
        # WRITE TO POSTGRESQL
        write_mode = "overwrite" if run_mode == "first" else "append"
        new_jenkins_builds.write.jdbc(CONNECTION_STR, table="jenkins_builds", mode = write_mode, properties=CONNECTION_PROPERTIES)
        new_sonar_measures.write.jdbc(CONNECTION_STR, table="sonar_measures", mode = write_mode, properties=CONNECTION_PROPERTIES)
        new_sonar_analyses.write.jdbc(CONNECTION_STR, table="sonar_analyses", mode = write_mode, properties=CONNECTION_PROPERTIES)
        new_sonar_issues.write.jdbc(CONNECTION_STR, table="sonar_issues", mode = write_mode, properties=CONNECTION_PROPERTIES)
        

    # APPLY MACHINE LEARNING
    apply_ml1(spark, new_jenkins_builds, db_jenkins_builds, new_sonar_measures, db_sonar_measures, new_sonar_analyses, db_sonar_analyses, spark_artefacts_dir, run_mode)
    apply_ml2(spark, new_jenkins_builds, db_jenkins_builds, new_sonar_issues, db_sonar_issues,  new_sonar_analyses, db_sonar_analyses, spark_artefacts_dir, run_mode)
    apply_ml3(spark, new_jenkins_builds, db_jenkins_builds, new_sonar_issues, db_sonar_issues,  new_sonar_analyses, db_sonar_analyses, spark_artefacts_dir, run_mode)

if __name__ == "__main__":

    ap = argparse.ArgumentParser(description="Central Spark processing")

    ap.add_argument("-j","--jenkins", default='../jenkins_data/data' , help="Path to Jenkins data folder")
    ap.add_argument("-s","--sonarqube", default='../sonarcloud_data/data' , help="Path to Sonarqube data folder")
    ap.add_argument("-a","--artefacts", default='./spark_artefacts' , help="Path to Spark artefacts folder")

    args = vars(ap.parse_args())

    jenkins_data_directory = args['jenkins']
    sonar_data_directory = args['sonarqube']
    spark_artefacts_dir = args['artefacts']

    # modes = ["first", "incremental", "update_models"]

    mode = "incremental"
    then = time.time()

    print(f"Start Spark processing - mode [{mode.upper()}]")

    run(jenkins_data_directory, sonar_data_directory, spark_artefacts_dir, mode)

    spark.stop()
    print(f"Time elapsed: {time.time() - then}")

