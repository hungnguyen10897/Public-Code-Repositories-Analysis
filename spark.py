# pyspark --driver-class-path postgresql-42.2.12.jar
# spark-submit --driver-class-path postgresql-42.2.12.jar spark.py

from pyspark import SparkContext, SparkConf
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import *
from pyspark.sql.utils import AnalysisException
from pyspark.sql.functions import col, udf
from pyspark.ml.feature import OneHotEncoderEstimator, StringIndexer, VectorAssembler, MinMaxScaler, Imputer
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.classification import LogisticRegression, LogisticRegressionModel, DecisionTreeClassifier, DecisionTreeClassificationModel, RandomForestClassifier, RandomForestClassificationModel
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

import numpy as np
from pathlib import Path
from collections import OrderedDict
import time
import sys

JENKINS_BUILD_DTYPE = OrderedDict({
    "job" : "object",
    "build_number" : "Int64",
    "result" : "object",
    "duration" : "Int64",
    "estimated_duration" : "Int64",
    "revision_number" : "object",
    "commit_id" : "object",
    "commit_ts" : "object",
    "test_pass_count" : "Int64",
    "test_fail_count" : "Int64",
    "test_skip_count" : "Int64",
    "total_test_duration" : "float64"
    })

JENKINS_TEST_DTYPE = OrderedDict({
    "job" : "object",
    "build_number" : "Int64",
    "package" : "object",
    "class" : "object",
    "name" : "object",
    "duration" : "float64",
    "status" : "object"})

SONAR_MEASURES_DTYPE = OrderedDict({
    'project': 'object',
    'analysis_key': 'object',
    'complexity': 'Int64',
    'class_complexity': 'object',
    'function_complexity': 'object',
    'file_complexity': 'float64',
    'function_complexity_distribution': 'object',
    'file_complexity_distribution': 'object',
    'complexity_in_classes': 'object',
    'complexity_in_functions': 'object',
    'cognitive_complexity': 'Int64',
    'test_errors': 'Int64',
    'skipped_tests': 'Int64',
    'test_failures': 'Int64',
    'tests': 'Int64',
    'test_execution_time': 'Int64',
    'test_success_density': 'float64',
    'coverage': 'float64',
    'lines_to_cover': 'Int64',
    'uncovered_lines': 'Int64',
    'line_coverage': 'float64',
    'conditions_to_cover': 'Int64',
    'uncovered_conditions': 'Int64',
    'branch_coverage': 'float64',
    'new_coverage': 'object',
    'new_lines_to_cover': 'object',
    'new_uncovered_lines': 'object',
    'new_line_coverage': 'object',
    'new_conditions_to_cover': 'object',
    'new_uncovered_conditions': 'object',
    'new_branch_coverage': 'object',
    'executable_lines_data': 'object',
    'public_api': 'object',
    'public_documented_api_density': 'object',
    'public_undocumented_api': 'object',
    'duplicated_lines': 'Int64',
    'duplicated_lines_density': 'float64',
    'duplicated_blocks': 'Int64',
    'duplicated_files': 'Int64',
    'duplications_data': 'object',
    'new_duplicated_lines': 'object',
    'new_duplicated_blocks': 'object',
    'new_duplicated_lines_density': 'object',
    'quality_profiles': 'object',
    'quality_gate_details': 'object',
    'violations': 'Int64',
    'blocker_violations': 'Int64',
    'critical_violations': 'Int64',
    'major_violations': 'Int64',
    'minor_violations': 'Int64',
    'info_violations': 'Int64',
    'new_violations': 'object',
    'new_blocker_violations': 'object',
    'new_critical_violations': 'object',
    'new_major_violations': 'object',
    'new_minor_violations': 'object',
    'new_info_violations': 'object',
    'false_positive_issues': 'Int64',
    'open_issues': 'Int64',
    'reopened_issues': 'Int64',
    'confirmed_issues': 'Int64',
    'wont_fix_issues': 'Int64',
    'sqale_index': 'Int64',
    'sqale_rating': 'float64',
    'development_cost': 'float64',
    'new_technical_debt': 'object',
    'sqale_debt_ratio': 'float64',
    'new_sqale_debt_ratio': 'float64',
    'code_smells': 'Int64',
    'new_code_smells': 'object',
    'effort_to_reach_maintainability_rating_a': 'Int64',
    'new_maintainability_rating': 'object',
    'new_development_cost': 'float64',
    'alert_status': 'object',
    'bugs': 'Int64',
    'new_bugs': 'object',
    'reliability_remediation_effort': 'Int64',
    'new_reliability_remediation_effort': 'object',
    'reliability_rating': 'float64',
    'new_reliability_rating': 'object',
    'last_commit_date': 'object',
    'vulnerabilities': 'Int64',
    'new_vulnerabilities': 'object',
    'security_remediation_effort': 'Int64',
    'new_security_remediation_effort': 'object',
    'security_rating': 'float64',
    'new_security_rating': 'object',
    'security_hotspots': 'Int64',
    'new_security_hotspots': 'object',
    'security_review_rating': 'float64',
    'classes': 'Int64',
    'ncloc': 'Int64',
    'functions': 'Int64',
    'comment_lines': 'Int64',
    'comment_lines_density': 'float64',
    'files': 'Int64',
    'directories': 'object',
    'lines': 'Int64',
    'statements': 'Int64',
    'generated_lines': 'object',
    'generated_ncloc': 'object',
    'ncloc_data': 'object',
    'comment_lines_data': 'object',
    'projects': 'object',
    'ncloc_language_distribution': 'object',
    'new_lines': 'object'})

SONAR_ISSUES_DTYPE = OrderedDict({
    "project" : "object",
    "current_analysis_key" : "object",
    "creation_analysis_key" : "object",
    "issue_key" : "object", 
    "type" : "object", 
    "rule" : "object", 
    "severity" : "object", 
    "status" : "object", 
    "resolution" : "object", 
    "effort" : "Int64", 
    "debt" : "Int64", 
    "tags" : "object", 
    "creation_date" : "object", 
    "update_date" : "object", 
    "close_date" :  "object"})

SONAR_ANALYSES_DTYPE = OrderedDict({
    "project" : "object", 
    "analysis_key" : "object", 
    "date" : "object", 
    "project_version" : "object", 
    "revision" : "object"})

# Drop these columns due to too many nulls
TO_DROP_SONAR_MEASURES_COLUMNS = [
    "class_complexity",
    "function_complexity",
    "function_complexity_distribution",
    "file_complexity_distribution",
    "complexity_in_classes",
    "complexity_in_functions",
    "new_coverage",
    "new_lines_to_cover",
    "new_uncovered_lines",
    "new_line_coverage",
    "new_conditions_to_cover",
    'new_uncovered_conditions',
    "new_branch_coverage",
    "executable_lines_data",
    "public_api",
    "public_documented_api_density",
    "public_undocumented_api",
    "duplications_data",
    "new_duplicated_lines",
    "new_duplicated_blocks",
    "new_duplicated_lines_density",
    "new_violations",
    "new_blocker_violations",
    "new_critical_violations",
    "new_major_violations",
    "new_minor_violations",
    "new_info_violations",
    "new_technical_debt",
    "new_code_smells",
    "new_maintainability_rating",
    "new_bugs",
    "new_reliability_remediation_effort",
    "new_reliability_rating",
    "new_vulnerabilities",
    "new_security_remediation_effort",
    "new_security_rating",
    "new_security_hotspots",
    "directories",
    "generated_lines",
    "generated_ncloc",
    "ncloc_data",
    "comment_lines_data",
    "projects",
    "new_lines",
]

JENKINS_BUILDS_NUMERICAL_COLUMNS = [
    "duration",
    "estimated_duration",
    "test_pass_count",
    "test_fail_count",
    "test_skip_count",
    "total_test_duration",
]

SONAR_MEASURES_NUMERICAL_COLUMNS = [
    "complexity",
    "file_complexity",
    "cognitive_complexity",
    "test_errors",
    "skipped_tests",
    "test_failures",
    "tests",
    "test_execution_time",
    "test_success_density",
    "coverage",
    "lines_to_cover",
    "uncovered_lines",
    "line_coverage",
    "conditions_to_cover",
    "uncovered_conditions",
    "branch_coverage",
    "duplicated_lines",
    "duplicated_lines_density",
    "duplicated_blocks",
    "duplicated_files",
    "violations",
    "blocker_violations",
    "critical_violations",
    'major_violations',
    "minor_violations",
    "info_violations",
    "false_positive_issues",
    "open_issues",
    "reopened_issues",
    "confirmed_issues",
    "wont_fix_issues",
    "sqale_index",
    "sqale_rating",
    "development_cost",
    "sqale_debt_ratio",
    "new_sqale_debt_ratio",
    "code_smells",
    "effort_to_reach_maintainability_rating_a",
    "new_development_cost",
    'bugs',
    "reliability_remediation_effort",
    "reliability_rating",
    "vulnerabilities",
    "security_remediation_effort",
    "security_rating",
    "security_hotspots",
    "security_review_rating",
    "classes",
    "ncloc",
    "functions",
    "comment_lines",
    "comment_lines_density",
    "files",
    "lines",
    "statements"
]

ML1_NUMERICAL_COLUMNS = JENKINS_BUILDS_NUMERICAL_COLUMNS + SONAR_MEASURES_NUMERICAL_COLUMNS

JENKINS_BUILDS_CATEGORICAL_COLUMNS = []

SONAR_MEASURES_CATEGORICAL_COLUMNS = [
    "alert_status"
]

ML1_CATEGORICAL_COLUMNS = JENKINS_BUILDS_CATEGORICAL_COLUMNS + SONAR_MEASURES_CATEGORICAL_COLUMNS

ML2_NUMERICAL_COLUMNS = [
    'introduced_blocker_code_smell', 
    'introduced_critical_code_smell', 
    'introduced_major_code_smell', 
    'introduced_minor_code_smell', 
    'introduced_info_code_smell', 
    'introduced_null_severity_code_smell', 
    'introduced_blocker_bug', 
    'introduced_critical_bug', 
    'introduced_major_bug', 
    'introduced_minor_bug', 
    'introduced_info_bug', 
    'introduced_null_severity_bug', 
    'introduced_blocker_vulnerability', 
    'introduced_critical_vulnerability', 
    'introduced_major_vulnerability', 
    'introduced_minor_vulnerability', 
    'introduced_info_vulnerability', 
    'introduced_null_severity_vulnerability', 
    'introduced_blocker_security_hotspot', 
    'introduced_critical_security_hotspot', 
    'introduced_major_security_hotspot', 
    'introduced_minor_security_hotspot', 
    'introduced_info_security_hotspot', 
    'introduced_null_severity_security_hotspot', 
    'removed_blocker_code_smell', 
    'removed_critical_code_smell', 
    'removed_major_code_smell', 
    'removed_minor_code_smell', 
    'removed_info_code_smell', 
    'removed_null_severity_code_smell', 
    'removed_blocker_bug', 
    'removed_critical_bug', 
    'removed_major_bug', 
    'removed_minor_bug', 
    'removed_info_bug', 
    'removed_null_severity_bug', 
    'removed_blocker_vulnerability', 
    'removed_critical_vulnerability', 
    'removed_major_vulnerability', 
    'removed_minor_vulnerability', 
    'removed_info_vulnerability', 
    'removed_null_severity_vulnerability', 
    'removed_blocker_security_hotspot', 
    'removed_critical_security_hotspot', 
    'removed_major_security_hotspot', 
    'removed_minor_security_hotspot', 
    'removed_info_security_hotspot', 
    'removed_null_severity_security_hotspot', 
    'current_blocker_code_smell', 
    'current_critical_code_smell', 
    'current_major_code_smell', 
    'current_minor_code_smell', 
    'current_info_code_smell', 
    'current_null_severity_code_smell', 
    'current_blocker_bug', 
    'current_critical_bug', 
    'current_major_bug', 
    'current_minor_bug', 
    'current_info_bug', 
    'current_null_severity_bug', 
    'current_blocker_vulnerability', 
    'current_critical_vulnerability', 
    'current_major_vulnerability', 
    'current_minor_vulnerability', 
    'current_info_vulnerability', 
    'current_null_severity_vulnerability', 
    'current_blocker_security_hotspot', 
    'current_critical_security_hotspot', 
    'current_major_security_hotspot', 
    'current_minor_security_hotspot', 
    'current_info_security_hotspot', 
    'current_null_severity_security_hotspot'
]

MODEL_PERFORMANCE_SCHEMA = StructType([
    StructField("model", StringType()),
    StructField("data_amount", IntegerType()),
    StructField("f1", FloatType()),
    StructField("weighted_precision", FloatType()),
    StructField("weighted_recall", FloatType()),
    StructField("accuracy", FloatType()),
    ])

CONNECTION_STR = "jdbc:postgresql://127.0.0.1:5432/pra"
CONNECTION_PROPERTIES = {"user": "pra", "password": "pra"}

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
            ignoreTrailingWhiteSpace = True, header=True, mode = 'FAILFAST').drop_duplicates()
    except AnalysisException:
        print(f"No .csv files for [{source}]")
        df = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)
    return df

def get_ml1_pipeline():
    stages = []

    imputer = Imputer(inputCols=ML1_NUMERICAL_COLUMNS , outputCols=ML1_NUMERICAL_COLUMNS )
    stages.append(imputer)

    ohe_input_cols = []
    ohe_output_cols = []
    for categorical_column in ML1_CATEGORICAL_COLUMNS:
        str_indexer = StringIndexer(inputCol=categorical_column, outputCol=categorical_column + "_index", handleInvalid='keep')
        ohe_input_cols.append(str_indexer.getOutputCol())
        ohe_output_cols.append(categorical_column + "_class_vec")
        stages.append(str_indexer)

    encoder = OneHotEncoderEstimator(inputCols=ohe_input_cols, outputCols=ohe_output_cols, handleInvalid="keep")
    stages.append(encoder)

    numerical_vector_assembler = VectorAssembler(inputCols=ML1_NUMERICAL_COLUMNS , outputCol="numerial_cols_vec", handleInvalid="keep")
    scaler = MinMaxScaler(inputCol="numerial_cols_vec", outputCol= "scaled_numerical_cols")
    stages.append(numerical_vector_assembler)
    stages.append(scaler)

    label_str_indexer = StringIndexer(inputCol="result", outputCol="label", handleInvalid="keep")
    stages.append(label_str_indexer)

    assembler_input = encoder.getOutputCols() + [scaler.getOutputCol()]
    assembler = VectorAssembler(inputCols= assembler_input, outputCol="features", handleInvalid="skip")
    stages.append(assembler)

    pipeline = Pipeline(stages = stages)
    return pipeline

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

def train_predict(df, spark_artefacts_dir, run_mode, i):

    pipeline_path = Path(spark_artefacts_dir).joinpath(f"pipeline_{i}")
    if run_mode == "first":
        pipeline = get_ml1_pipeline() if i == 1 else get_ml2_pipeline()
        pipeline_model = pipeline.fit(df)
        pipeline_model.write().overwrite().save(str(pipeline_path.absolute()))

        ml_df = pipeline_model.transform(df).select('features','label')

        train,test = ml_df.randomSplit([0.7, 0.3])
        train.persist()
        test.persist()

        train_count = train.count()
        print("Training Dataset Count: " + str(train_count))
        test_count = test.count()
        print("Test Dataset Count: " + str(test_count))

        lr = LogisticRegression(featuresCol='features', labelCol='label', maxIter=10)
        dt = DecisionTreeClassifier(featuresCol='features', labelCol='label', maxDepth=5)
        rf = RandomForestClassifier(featuresCol = 'features', labelCol = 'label', numTrees=100)

        model_performance_lines = []
        for algo, model_name in [(lr,f"LogisticRegressionModel_{i}"),(dt,f"DecisionTreeModel_{i}"),(rf,f"RandomForestModel_{i}")]:

            print(model_name)
            model = algo.fit(train)
            model_path = Path(spark_artefacts_dir).joinpath(model_name)
            model.write().overwrite().save(str(model_path.absolute()))
            
            predictions = model.transform(test)
            predictions.persist()
            predictions.show(5)

            measures = []
            evaluator = MulticlassClassificationEvaluator()
            for metricName in ["f1","weightedPrecision","weightedRecall","accuracy"]:
                measure = evaluator.evaluate(predictions, {evaluator.metricName: metricName})
                measures.append(measure)
                print(f"\t{metricName}: {measure}")
            
            model_performance_lines.append([model_name, test_count] + measures)
        
        model_performance_df = spark.createDataFrame(data= model_performance_lines, schema = MODEL_PERFORMANCE_SCHEMA)
        model_performance_df.write.jdbc(CONNECTION_STR, 'model_performance', mode = 'overwrite', properties= CONNECTION_PROPERTIES)

    elif run_mode == "incremental":
        pipeline_model = PipelineModel.load(str(pipeline_path.absolute()))
        ml_df = pipeline_model.transform(df).select('features','label')

        ml_df.persist()
    
        model_performance_lines = []
        for model, name in [(LogisticRegressionModel, f"LogisticRegressionModel_{i}"), (DecisionTreeClassificationModel, f"DecisionTreeModel_{i}"), (RandomForestClassificationModel, f"RandomForestModel_{i}")]:
            
            print("\n\n" + name)
            model_path = Path(spark_artefacts_dir).joinpath(name)
            ml_model = model.load(str(model_path.absolute()))

            predictions = ml_model.transform(ml_df)
            predictions.persist()
            predictions.show(5)

            measures = []
            evaluator = MulticlassClassificationEvaluator()
            for metricName in ["f1","weightedPrecision","weightedRecall","accuracy"]:
                measure = evaluator.evaluate(predictions, {evaluator.metricName: metricName})
                measures.append(measure)
                print(f"\t{metricName}: {measure}")

            model_performance_lines.append([model_name, test_count] + measures) 

        model_performance_df = spark.createDataFrame(data= model_performance_lines, schema = MODEL_PERFORMANCE_SCHEMA)
        model_performance_df.write.jdbc(CONNECTION_STR, 'model_performance', mode = 'append', properties = CONNECTION_PROPERTIES)

def apply_ml1(new_jenkins_builds, db_jenkins_builds, new_sonar_measures, db_sonar_measures, new_sonar_analyses, db_sonar_analyses, spark_artefacts_dir, run_mode):

    # PREPARE DATA
    if run_mode == "first":

        ml_sonar_df = new_sonar_measures.join(new_sonar_analyses, new_sonar_measures.analysis_key == new_sonar_analyses.analysis_key, 
        how = 'inner').select(*(['revision'] + SONAR_MEASURES_NUMERICAL_COLUMNS + SONAR_MEASURES_CATEGORICAL_COLUMNS))
        df = new_jenkins_builds.join(ml_sonar_df, new_jenkins_builds.revision_number == ml_sonar_df.revision, how = 'inner')

    elif run_mode == "incremental":
        
        # New jenkins ~ db sonar
        ml_sonar_df_1 = db_sonar_measures.join(db_sonar_analyses, db_sonar_measures.analysis_key == db_sonar_analyses.analysis_key, 
        how = 'inner').select(*(['revision'] + SONAR_MEASURES_NUMERICAL_COLUMNS + SONAR_MEASURES_CATEGORICAL_COLUMNS))
        df1 = new_jenkins_builds.join(ml_sonar_df_1, new_jenkins_builds.revision_number == ml_sonar_df_1.revision, how = 'inner')

        # New sonar ~ db jenkins
        ml_sonar_df_2 = new_sonar_measures.join(db_sonar_analyses, new_sonar_measures.analysis_key == db_sonar_analyses.analysis_key, 
        how = 'inner').select(*(['revision'] + SONAR_MEASURES_NUMERICAL_COLUMNS + SONAR_MEASURES_CATEGORICAL_COLUMNS))
        df2 = db_jenkins_builds.join(ml_sonar_df_2, db_jenkins_builds.revision_number == ml_sonar_df_2.revision, how = 'inner')

        df = df1.union(df2).drop_duplicates()

    # Change data type from Int to Float to fit into estimators
    for column_name in ML1_NUMERICAL_COLUMNS:
        if column_name in JENKINS_BUILD_DTYPE:
            if JENKINS_BUILD_DTYPE[column_name] == 'Int64':
                df = df.withColumn(column_name, df[column_name].astype(DoubleType()))
        elif column_name in SONAR_MEASURES_DTYPE:
            if SONAR_MEASURES_DTYPE[column_name] == 'Int64':
                df = df.withColumn(column_name, df[column_name].astype(DoubleType()))

    df.persist()  
    print(f"DF for ML1 Count: {str(df.count())}")

    train_predict(df, spark_artefacts_dir, run_mode, 1)

def apply_ml2(new_jenkins_builds, db_jenkins_builds, new_sonar_issues, db_sonar_issues,  new_sonar_analyses, db_sonar_analyses, spark_artefacts_dir, run_mode):

    with open('./sonar_issues_count.sql', 'r') as f:
        query1 = f.read()
    with open('./sonar_issues_count_with_current.sql', 'r') as f:
        query2 = f.read()

    #PREPARE DATA
    if run_mode == "first":
        new_sonar_issues.createOrReplaceTempView('sonar_issues')
        sonar_issues_count = spark.sql(query1)
        sonar_issues_count.createOrReplaceTempView('sonar_issues_count')
        sonar_issues_count_with_current = spark.sql(query2)
        sonar_df = sonar_issues_count_with_current.join(new_sonar_analyses, sonar_issues_count_with_current.analysis_key == new_sonar_analyses.analysis_key,
            how = "inner")
        df = sonar_df.join(new_jenkins_builds, sonar_df.revision == new_jenkins_builds.revision_number, how = "inner").select(*(['result'] + ML2_NUMERICAL_COLUMNS))

    elif run_mode == "incremental":

        # New jenkins ~ db sonar
        db_sonar_issues.createOrReplaceTempView('sonar_issues')
        sonar_issues_count_1 = spark.sql(query1)
        sonar_issues_count_1.createOrReplaceTempView('sonar_issues_count')
        sonar_issues_count_with_current_1 = spark.sql(query2)
        sonar_df_1 = sonar_issues_count_with_current_1.join(db_sonar_analyses, sonar_issues_count_with_current_1.analysis_key == db_sonar_analyses.analysis_key,
            how = "inner")
        df1 = sonar_df_1.join(new_jenkins_builds, sonar_df_1.revision == new_jenkins_builds.revision_number, how = "inner").select(*(['result'] + ML2_NUMERICAL_COLUMNS))

        # New sonar ~ db jenkins
        new_sonar_issues.createOrReplaceTempView('sonar_issues')
        sonar_issues_count_2 = spark.sql(query1)
        sonar_issues_count_2.createOrReplaceTempView('sonar_issues_count')
        sonar_issues_count_with_current_2 = spark.sql(query2)
        sonar_df_2 = sonar_issues_count_with_current_2.join(db_sonar_analyses, sonar_issues_count_with_current_2.analysis_key == db_sonar_analyses.analysis_key,
            how = "inner")
        df2 = sonar_df_2.join(db_jenkins_builds, sonar_df_2.revision == db_jenkins_builds.revision_number, how = "inner").select(*(['result'] + ML2_NUMERICAL_COLUMNS))

        df = df1.union(df2).drop_duplicates()

    # Change data types to fit in estimators
    for numerical_column in ML2_NUMERICAL_COLUMNS:
        df = df.withColumn(numerical_column, df[numerical_column].astype(DoubleType()))
    
    df.persist()
    print(f"DF for ML2 Count: {str(df.count())}")

    train_predict(df, spark_artefacts_dir, run_mode, 2)

def run(jenkins_data_directory, sonar_data_directory, spark_artefacts_dir, run_mode):

    # Check for resources that enable incremental run
    if run_mode == "incremental":
        for i in ['1','2']:
            for obj in [f"pipeline_{i}",f"LogisticRegressionModel_{i}",f"DecisionTreeModel_{i}",f"RandomForestModel_{i}"]:
                obj_path = Path(spark_artefacts_dir).joinpath(obj)
                if not obj_path.exists():
                    print(f"{obj} does not exist in spark_artefacts. Rerun with run_mode = first")
                    run(jenkins_data_directory, sonar_data_directory, spark_artefacts_dir, "first")

        # Data from db
        try:
            db_jenkins_builds = spark.read.jdbc(CONNECTION_STR, "jenkins_builds", properties=CONNECTION_PROPERTIES)
            db_sonar_analyses = spark.read.jdbc(CONNECTION_STR, "sonar_analyses", properties=CONNECTION_PROPERTIES) 
            db_sonar_measures = spark.read.jdbc(CONNECTION_STR, "sonar_measures", properties=CONNECTION_PROPERTIES) 
            db_sonar_issues = spark.read.jdbc(CONNECTION_STR, "sonar_issues", properties=CONNECTION_PROPERTIES) 

            for table,name in [(db_jenkins_builds,"jenkins_builds"), (db_sonar_analyses, "sonar_analyses"), (db_sonar_measures, "sonar_measures"), (db_sonar_issues, "sonar_issues")]:
                if table.count() == 0:
                    print(f"No data in table [{name}]. Rerun with run_mode = first")
                    run(jenkins_data_directory, sonar_data_directory, spark_artefacts_dir, "first")
                    
        except Exception as e:
            print(f"Exception thrown when reading tables from Postgresql - {str(e)}. Rerun with run_mode = first")
            run(jenkins_data_directory, sonar_data_directory, spark_artefacts_dir, "first")

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

    # WRITE TO POSTGRESQL
    write_mode = "overwrite" if run_mode == "first" else "append"
    new_jenkins_builds.write.jdbc(CONNECTION_STR, table="jenkins_builds", mode = write_mode, properties=CONNECTION_PROPERTIES)
    new_sonar_measures.write.jdbc(CONNECTION_STR, table="sonar_measures", mode = write_mode, properties=CONNECTION_PROPERTIES)
    new_sonar_analyses.write.jdbc(CONNECTION_STR, table="sonar_analyses", mode = write_mode, properties=CONNECTION_PROPERTIES)
    new_sonar_issues.write.jdbc(CONNECTION_STR, table="sonar_issues", mode = write_mode, properties=CONNECTION_PROPERTIES)

    # APPLY MACHINE LEARNING
    apply_ml1(new_jenkins_builds, db_jenkins_builds, new_sonar_measures, db_sonar_measures, new_sonar_analyses, db_sonar_analyses, spark_artefacts_dir, run_mode)
    apply_ml2(new_jenkins_builds, db_jenkins_builds, new_sonar_issues, db_sonar_issues,  new_sonar_analyses, db_sonar_analyses, spark_artefacts_dir, run_mode)

if __name__ == "__main__":

    jenkins_data_directory = "./jenkins_data/data"
    sonar_data_directory = "./sonarcloud_data/data"
    spark_artefacts_dir = "./spark_artefacts"

    mode = "first"
    then = time.time()
    print(f"Start Spark processing - mode: {mode.upper()}")

    run(jenkins_data_directory, sonar_data_directory, spark_artefacts_dir, mode)

    spark.stop()
    print(f"Time elapsed: {time.time() - then}")
