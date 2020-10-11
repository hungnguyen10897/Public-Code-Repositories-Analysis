# pyspark --driver-class-path postgresql-42.2.12.jar
# spark-submit --driver-class-path postgresql-42.2.12.jar spark.py

from pyspark import SparkContext, SparkConf
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import Row
from pyspark.sql.utils import AnalysisException
from pyspark.sql.functions import udf
from pyspark.ml.feature import OneHotEncoderEstimator, StringIndexer, StringIndexerModel, VectorAssembler, MinMaxScaler, Imputer, ChiSqSelector, ChiSqSelectorModel
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
from common import train_predict, feature_selector_process, pipeline_process

#to be updated later
ML3_COLUMNS = []

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

def get_ml2_pipeline():
    stages = []

    numerical_vector_assembler = VectorAssembler(inputCols=ML2_NUMERICAL_COLUMNS , outputCol="numerial_cols_vec", handleInvalid="keep")
    scaler = MinMaxScaler(inputCol="numerial_cols_vec", outputCol= "features")
    stages.append(numerical_vector_assembler)
    stages.append(scaler)

    label_str_indexer = StringIndexer(inputCol="result", outputCol="label", handleInvalid="keep")
    stages.append(label_str_indexer)

    pipeline = Pipeline(stages = stages)
    return pipeline

def get_ml3_pipeline():

    stages = []
    str_idx = StringIndexer(inputCol="rule", outputCol="rule_idx", handleInvalid="skip")
    ohe = OneHotEncoderEstimator(inputCols=["rule_idx"], outputCols=["rule_vec"], dropLast=False)
    stages = [str_idx, ohe]
    return Pipeline(stages= stages)

def prepare_data_ml2(jenkins_builds, sonar_issues, sonar_analyses):

    with open('./sonar_issues_count.sql', 'r') as f:
        query1 = f.read()
    with open('./sonar_issues_count_with_current.sql', 'r') as f:
        query2 = f.read()

    sonar_issues.createOrReplaceTempView('sonar_issues')
    sonar_issues_count = spark.sql(query1)
    sonar_issues_count.createOrReplaceTempView('sonar_issues_count')
    sonar_issues_count_with_current = spark.sql(query2)
    sonar_df = sonar_issues_count_with_current.join(sonar_analyses, sonar_issues_count_with_current.analysis_key == sonar_analyses.analysis_key,
        how = "inner")
    df = sonar_df.join(jenkins_builds, sonar_df.revision == jenkins_builds.revision_number, how = "inner").select(*(['result'] + ML2_NUMERICAL_COLUMNS))

    # Change data types to fit in estimators
    for numerical_column in ML2_NUMERICAL_COLUMNS:
        df = df.withColumn(numerical_column, df[numerical_column].astype(DoubleType()))

    return df

def apply_ml2(new_jenkins_builds, db_jenkins_builds, new_sonar_issues, db_sonar_issues,  new_sonar_analyses, db_sonar_analyses, spark_artefacts_dir, run_mode):

    modify_result = udf(lambda x: "SUCCESS" if x == "SUCCESS" else "FAIL", StringType())
    spark.udf.register("modify_result" , modify_result)

    if new_jenkins_builds is not None:
        new_jenkins_builds = new_jenkins_builds.withColumn("result", modify_result("result"))

    if db_jenkins_builds is not None:
        db_jenkins_builds = db_jenkins_builds.withColumn("result", modify_result("result"))

    if run_mode == "first":
        df = prepare_data_ml2(new_jenkins_builds, new_sonar_issues, new_sonar_analyses)

    elif run_mode == "incremental":

        # New jenkins ~ db sonar
        df1 = prepare_data_ml2(new_jenkins_builds, db_sonar_issues, db_sonar_analyses)

        # New sonar ~ db jenkins
        df2 = prepare_data_ml2(db_jenkins_builds, new_sonar_issues, db_sonar_analyses)

        df = df1.union(df2)
    
    df.persist()
    print(f"DF for ML2 Count: {str(df.count())}")
    if df.count() == 0:
        print("No data for ML2 - Returning...")
        return

    ml_df,_ = pipeline_process(df, get_ml2_pipeline, spark_artefacts_dir, run_mode, 2)
    ml_df.persist()
    ml_df_10, ml_10_columns = feature_selector_process(spark, ml_df, spark_artefacts_dir, run_mode, 2, ML2_NUMERICAL_COLUMNS)
    ml_df_10.persist()

    train_predict(spark, ml_df, spark_artefacts_dir, run_mode, 2, ML2_NUMERICAL_COLUMNS)
    train_predict(spark, ml_df_10, spark_artefacts_dir, run_mode, 2, ml_10_columns, True)

def sum_sparse_vectors(v1, v2):

    def add_to_map(m, e):
        key = e[0]
        value = e[1]
        if key in m:
            m[key] = m[key] + value
        else:
            m[key] = value
        return m
    results = OrderedDict()

    v = list(zip(v1.indices, v1.values)) + list(zip(v2.indices, v2.values))
    v.sort(key = lambda x: x[0])
    for e in v:
        results = add_to_map(results, e)
    return SparseVector(v1.size, list(results.keys()), list(results.values()))

def prepre_data_ml3(jenkins_builds, sonar_issues, sonar_analyses ,pipeline_model, label_idx_model):
    
    removed_rules_df = sonar_issues.filter("status IN ('RESOLVED', 'CLOSED', 'REVIEWED')").select("current_analysis_key","rule")
    df1 = pipeline_model.transform(removed_rules_df)
    rdd1 = df1.rdd.map(lambda x : (x[0],x[3])).reduceByKey(lambda v1,v2: sum_sparse_vectors(v1,v2)) \
                                                .map(lambda x: Row(current_analysis_key = x[0], removed_rule_vec = x[1]))
    if rdd1.count() == 0:
        return None
    removed_issues_rule_vec_df = spark.createDataFrame(rdd1)

    introduced_rules_df = sonar_issues.filter("status IN ('OPEN', 'REOPENED', 'CONFIRMED', 'TO_REVIEW')").select("creation_analysis_key","rule")
    df2 = pipeline_model.transform(introduced_rules_df)
    rdd2 = df2.rdd.map(lambda x : (x[0],x[3])).reduceByKey(lambda v1,v2: sum_sparse_vectors(v1,v2)) \
                                                .map(lambda x: Row(creation_analysis_key = x[0], introduced_rule_vec = x[1]))
    if rdd2.count() == 0:
        return None                                                
    introduced_issues_rule_vec_df = spark.createDataFrame(rdd2)

    joined_sonar_rules_df = removed_issues_rule_vec_df.join(introduced_issues_rule_vec_df, 
        removed_issues_rule_vec_df.current_analysis_key == introduced_issues_rule_vec_df.creation_analysis_key, how = "outer")

    joined_sonar_rules_df.createOrReplaceTempView("sonar_rules")
    joined_sonar_rules_df = spark.sql("""SELECT 
        coalesce(current_analysis_key, creation_analysis_key) AS analysis_key,
        introduced_rule_vec,
        removed_rule_vec
        FROM sonar_rules
        """)

    num_rules = len(pipeline_model.stages[0].labels)

    imputed_sonar_rules_rdd = joined_sonar_rules_df.rdd.map(lambda row: Row(
        analysis_key = row[0], 
        introduced_rule_vec = SparseVector(num_rules,{}) if row[1] is None else row[1], 
        removed_rule_vec = SparseVector(num_rules,{}) if row[2] is None else row[2]))

    imputed_sonar_rules_df = spark.createDataFrame(imputed_sonar_rules_rdd)        
    
    v_assembler = VectorAssembler(inputCols=["removed_rule_vec", "introduced_rule_vec"], outputCol="features")
    sonar_issues_df = v_assembler.transform(imputed_sonar_rules_df).select("analysis_key","features")

    sonar_df = sonar_issues_df.join(sonar_analyses, sonar_issues_df.analysis_key == sonar_analyses.analysis_key, how = "inner")
    df = sonar_df.join(jenkins_builds, sonar_df.revision == jenkins_builds.revision_number, how = "inner").select("result", "features")
    ml_df = label_idx_model.transform(df).select("label", "features")

    return ml_df

def apply_ml3(new_jenkins_builds, db_jenkins_builds, new_sonar_issues, db_sonar_issues,  new_sonar_analyses, db_sonar_analyses, spark_artefacts_dir, run_mode):

    # Change build result to only SUCCESS/FAIL for binary classification
    modify_result = udf(lambda x: "SUCCESS" if x == "SUCCESS" else "FAIL", StringType())
    spark.udf.register("modify_result" , modify_result)

    if new_jenkins_builds is not None:
        new_jenkins_builds = new_jenkins_builds.withColumn("result", modify_result("result"))

    if db_jenkins_builds is not None:
        db_jenkins_builds = db_jenkins_builds.withColumn("result", modify_result("result"))

    pipeline_path = Path(spark_artefacts_dir).joinpath("pipeline_3")
    label_idx_model_path = Path(spark_artefacts_dir).joinpath("label_indexer_3")
    #PREPARE DATA
    if run_mode == "first":

        pipeline_model = get_ml3_pipeline().fit(new_sonar_issues)
        pipeline_model.write().overwrite().save(str(pipeline_path.absolute()))

        label_idx_model = StringIndexer(inputCol="result", outputCol="label", handleInvalid="skip").fit(new_jenkins_builds)
        label_idx_model.write().overwrite().save(str(label_idx_model_path.absolute()))

        ml_df = prepre_data_ml3(new_jenkins_builds, new_sonar_issues, new_sonar_analyses, pipeline_model, label_idx_model)
        if ml_df is None:
            print("No data for ML3 - Returning...")
            return

    elif run_mode == "incremental":
        
        pipeline_model = PipelineModel.load(str(pipeline_path.absolute()))
        label_idx_model = StringIndexerModel.load(str(label_idx_model_path.absolute()))

        ml_df1 = prepre_data_ml3(new_jenkins_builds, db_sonar_issues, db_sonar_analyses, pipeline_model, label_idx_model)
        ml_df2 = prepre_data_ml3(db_jenkins_builds, new_sonar_issues, db_sonar_analyses, pipeline_model, label_idx_model)
        if ml_df1 is None or ml_df2 is None:
            print("No data for ML3 - Returning...")
            return

        ml_df = ml_df1.union(ml_df2)

    rules = pipeline_model.stages[0].labels
    global ML3_COLUMNS
    ML3_COLUMNS = list(map(lambda x: "removed_" + x, rules)) + list(map(lambda x: "introduced_" + x, rules))

    ml_df.persist()
    print(f"DF for ML3 Count: {ml_df.count()}")
    if ml_df.count() == 0:
        print("No data for ML3 - Returning...")
        return

    ml_df_10, ml_10_columns = feature_selector_process(spark, ml_df, spark_artefacts_dir, run_mode, 3, ML3_COLUMNS)
    ml_df_10.persist()

    train_predict(spark, ml_df, spark_artefacts_dir, run_mode, 3, ML3_COLUMNS)
    train_predict(spark, ml_df_10, spark_artefacts_dir, run_mode, 3, ml_10_columns, True)

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
    apply_ml2(new_jenkins_builds, db_jenkins_builds, new_sonar_issues, db_sonar_issues,  new_sonar_analyses, db_sonar_analyses, spark_artefacts_dir, run_mode)
    apply_ml3(new_jenkins_builds, db_jenkins_builds, new_sonar_issues, db_sonar_issues,  new_sonar_analyses, db_sonar_analyses, spark_artefacts_dir, run_mode)

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

