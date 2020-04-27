# pyspark --driver-class-path postgresql-42.2.12.jar
# spark-submit --driver-class-path postgresql-42.2.12.jar spark.py

from pyspark import SparkContext, SparkConf
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import *
from pyspark.sql.types import Row
from pyspark.sql.utils import AnalysisException
from pyspark.sql.functions import col, udf
from pyspark.ml.feature import OneHotEncoderEstimator, StringIndexer, StringIndexerModel, VectorAssembler, MinMaxScaler, Imputer, ChiSqSelector, ChiSqSelectorModel
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.classification import LogisticRegression, LogisticRegressionModel, DecisionTreeClassifier, DecisionTreeClassificationModel, RandomForestClassifier, RandomForestClassificationModel
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, BinaryClassificationEvaluator
from pyspark.ml.linalg import *
from pyspark.ml.stat import ChiSquareTest
import pandas as pd

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

# to be updated later
ML1_COLUMNS = []

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

#to be updated later
ML3_COLUMNS = []

MODEL_PERFORMANCE_SCHEMA = StructType([
    StructField("model", StringType()),
    StructField("data_amount", IntegerType()),
    StructField("f1", FloatType()),
    StructField("weighted_precision", FloatType()),
    StructField("weighted_recall", FloatType()),
    StructField("accuracy", FloatType()),
    StructField("area_under_ROC", FloatType()),
    StructField("area_under_PR", FloatType()),
    StructField("predicted_negative_rate", FloatType()),
    ])

MODEL_INFO_SCHEMA = StructType([
    StructField("model", StringType()),
    StructField("train_data_amount", IntegerType()),
    StructField("train_f1", FloatType()),
    StructField("train_weighted_precision", FloatType()),
    StructField("train_weighted_recall", FloatType()),
    StructField("train_accuracy", FloatType()),
    StructField("train_area_under_ROC", FloatType()),
    StructField("train_area_under_PR", FloatType()),
    StructField("train_predicted_negative_rate", FloatType()),
    StructField("feature_1", StringType()),
    StructField("feature_importance_1", FloatType()),
    StructField("feature_2", StringType()),
    StructField("feature_importance_2", FloatType()),
    StructField("feature_3", StringType()),
    StructField("feature_importance_3", FloatType()),
    StructField("feature_4", StringType()),
    StructField("feature_importance_4", FloatType()),
    StructField("feature_5", StringType()),
    StructField("feature_importance_5", FloatType()),
    StructField("feature_6", StringType()),
    StructField("feature_importance_6", FloatType()),
    StructField("feature_7", StringType()),
    StructField("feature_importance_7", FloatType()),
    StructField("feature_8", StringType()),
    StructField("feature_importance_8", FloatType()),
    StructField("feature_9", StringType()),
    StructField("feature_importance_9", FloatType()),
    StructField("feature_10", StringType()),
    StructField("feature_importance_10", FloatType()),
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
            ignoreTrailingWhiteSpace = True, header=True, mode = 'FAILFAST')
    except AnalysisException:
        print(f"No .csv files for [{source}].")
        df = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)
    return df.drop_duplicates()

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

    encoder = OneHotEncoderEstimator(inputCols=ohe_input_cols, outputCols=ohe_output_cols, handleInvalid="error", dropLast=False)
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

def get_ml3_pipeline():

    stages = []
    str_idx = StringIndexer(inputCol="rule", outputCol="rule_idx")
    ohe = OneHotEncoderEstimator(inputCols=["rule_idx"], outputCols=["rule_vec"], dropLast=False)
    stages = [str_idx, ohe]
    return Pipeline(stages= stages)

def pipeline_process(df, spark_artefacts_dir, run_mode, i):

    pipeline_path = Path(spark_artefacts_dir).joinpath(f"pipeline_{i}")
    if run_mode == "first":
        pipeline = get_ml1_pipeline() if i == 1 else get_ml2_pipeline()
        pipeline_model = pipeline.fit(df)
        pipeline_model.write().overwrite().save(str(pipeline_path.absolute()))

    elif run_mode == "incremental":
        pipeline_model = PipelineModel.load(str(pipeline_path.absolute()))

    return pipeline_model.transform(df).select('features','label'), pipeline_model

def get_categorical_columns(ohe_category_sizes):

    cols = []
    for column, size in zip(ML1_CATEGORICAL_COLUMNS, ohe_category_sizes):
        for i in range(size):
            cols.append(f"{column}_index_{str(i)}")

    return cols

def feature_selector_process(ml_df, spark_artefacts_dir, run_mode, i):

    # APPLY CHI-SQUARE SELECTOR
    name = f"ChiSquareSelectorModel_{i}"
    selector_model_path = Path(spark_artefacts_dir).joinpath(name)

    feature_cols = []
    if i == 1:
        feature_cols = ML1_COLUMNS
    elif i == 2:
        feature_cols = ML2_NUMERICAL_COLUMNS
    elif i == 3:
        feature_cols = ML3_COLUMNS

    if run_mode == 'first':

        # ChiSq Test to obtain ChiSquare values (higher -> more dependence between feature and lable -> better)
        r = ChiSquareTest.test(ml_df, "features", "label")
        pValues = r.select("pvalues").collect()[0][0].tolist()
        stats = r.select("statistics").collect()[0][0].tolist()
        dof = r.select("degreesOfFreedom").collect()[0][0]

        # ChiSq Selector
        selector =ChiSqSelector(numTopFeatures= 10, featuresCol="features", outputCol="selected_features", labelCol="label")
        selector_model = selector.fit(ml_df)       
        selector_model.write().overwrite().save(str(selector_model_path.absolute()))

        top_10_feaures_importance = []
        top_10_features = []
        for j in selector_model.selectedFeatures:
            top_10_feaures_importance.append(feature_cols[j])
            top_10_features.append(feature_cols[j])
            top_10_feaures_importance.append(stats[j])

        model_info = [name, ml_df.count(), None, None, None, None, None, None, None] + top_10_feaures_importance
        model_info_df = spark.createDataFrame(data = [model_info], schema = MODEL_INFO_SCHEMA)
        model_info_df.write.jdbc(CONNECTION_STR, 'model_info', mode='append', properties=CONNECTION_PROPERTIES)

    elif run_mode == 'incremental':
        selector_model = ChiSqSelectorModel.load(str(selector_model_path.absolute()))
        top_10_features = []
        for j in selector_model.selectedFeatures:
            top_10_features.append(feature_cols[j])

    ml_df_10 = selector_model.transform(ml_df)
    ml_df_10 = ml_df_10.drop("features")

    #Solve a problem with ChiSqSelector and Tree-based algorithm
    ml_rdd_10 = ml_df_10.rdd.map(lambda row: Row(label = row[0], features =DenseVector(row[1].toArray())))
    ml_df_10 = spark.createDataFrame(ml_rdd_10)

    return ml_df_10, top_10_features

def train_predict(ml_df, spark_artefacts_dir, run_mode, i, top_10_columns):

    if top_10_columns is not None:
        ML_COLUMNS = top_10_columns
    else:
        if i == 1:
            ML_COLUMNS = ML1_COLUMNS
        elif i == 2:
            ML_COLUMNS = ML2_NUMERICAL_COLUMNS
        elif i == 3:
            ML_COLUMNS = ML3_COLUMNS

    suffix = "_top_10" if top_10_columns is not None else ""
    lr_model_name = f"LogisticRegressionModel_{i}" + suffix
    dt_model_name = f"DecisionTreeModel_{i}" + suffix
    rf_model_name = f"RandomForestModel_{i}" + suffix

    if run_mode == "first":

        train,test = ml_df.randomSplit([0.7, 0.3])
        train.persist()
        test.persist()

        replication_factor = 10
        negative_label_train = train.filter("label = 1.0")
        negative_label_train.persist()
        negative_label_train.collect()

        for i in range(replication_factor):
            train = train.union(negative_label_train)
            train.persist()
            
        train.persist()
        train_count = train.count()
        print("Training Dataset Count: " + str(train_count))
        test_count = test.count()
        print("Test Dataset Count: " + str(test_count))

        lr = LogisticRegression(featuresCol='features', labelCol='label', maxIter=10)
        dt = DecisionTreeClassifier(featuresCol='features', labelCol='label', maxDepth=5)
        rf = RandomForestClassifier(featuresCol = 'features', labelCol = 'label', numTrees=100)

        model_performance_lines = []
        model_info_lines = []
        for algo, model_name in [(lr, lr_model_name), (dt, dt_model_name), (rf,rf_model_name)]:
            print(model_name)
            model = algo.fit(train)
            model_path = Path(spark_artefacts_dir).joinpath(model_name)
            model.write().overwrite().save(str(model_path.absolute()))

            # Tree-based algorithm's Feature Importances:
            if algo in [dt, rf]:

                f_importances = model.featureImportances
                indices = f_importances.indices.tolist()
                values = f_importances.values.tolist()

                value_index_lst = list(zip(values, indices))
                value_index_lst.sort(key = lambda x: x[0], reverse= True)
                
                importance_sorted_features = []
                for value, index in value_index_lst:
                    importance_sorted_features.append(ML_COLUMNS[index])
                    importance_sorted_features.append(value)

                length = len(importance_sorted_features)
                
                if length > 20:
                    importance_sorted_features = importance_sorted_features[:20]
                elif length < 20:
                    importance_sorted_features = importance_sorted_features + (20 - length)*[None]

            else:
                importance_sorted_features = 20 * [None]
            
            predictions = model.transform(test)
            predictions.persist()
            predictions.show(5)

            train_predictions = model.transform(train)
            train_predictions.persist()
            train_predictions.show(5)

            measures = []
            train_measures = []

            ma_eval = MulticlassClassificationEvaluator()
            for metricName in ["f1","weightedPrecision","weightedRecall","accuracy"]:
                measure = ma_eval.evaluate(predictions, {ma_eval.metricName: metricName})
                measures.append(measure)
                print(f"\t{metricName}: {measure}")

                train_measure = ma_eval.evaluate(train_predictions, {ma_eval.metricName: metricName})
                train_measures.append(train_measure)
                print(f"\tTrain-{metricName}: {train_measure}")

            bin_eval = BinaryClassificationEvaluator()
            for metricName in ["areaUnderROC" , "areaUnderPR"]:
                measure = bin_eval.evaluate(predictions, {bin_eval.metricName: metricName})
                measures.append(measure)
                print(f"\t{metricName}: {measure}")

                train_measure = bin_eval.evaluate(train_predictions, {bin_eval.metricName: metricName})
                train_measures.append(train_measure)
                print(f"\tTrain-{metricName}: {train_measure}")

            # Predicted negatives
            predicted_negative_rate = predictions.select("label").filter("label = 1.0 AND prediction = 1.0").count() \
                / predictions.select("label").filter("label = 1.0").count()
            print(f"\tpredicted_negative_rate: {predicted_negative_rate}")
            measures.append(predicted_negative_rate)

            train_predicted_negative_rate = train_predictions.select("label").filter("label = 1.0 AND prediction = 1.0").count() \
                / train_predictions.select("label").filter("label = 1.0").count()
            print(f"\ttrain_predicted_negative_rate: {train_predicted_negative_rate}")
            train_measures.append(train_predicted_negative_rate)
            
            model_performance_lines.append([model_name, test_count] + measures)
            model_info_lines.append([model_name, train_count] + train_measures + importance_sorted_features)
            
        model_performance_df = spark.createDataFrame(data= model_performance_lines, schema = MODEL_PERFORMANCE_SCHEMA)
        model_performance_df.write.jdbc(CONNECTION_STR, 'model_performance', mode = 'append', properties= CONNECTION_PROPERTIES)

        model_info_df = spark.createDataFrame(data= model_info_lines, schema = MODEL_INFO_SCHEMA)
        model_info_df.write.jdbc(CONNECTION_STR, 'model_info', mode = 'append', properties= CONNECTION_PROPERTIES)

    elif run_mode == "incremental":
    
        model_performance_lines = []
        for model, name in [(LogisticRegressionModel, lr_model_name), (DecisionTreeClassificationModel, dt_model_name), (RandomForestClassificationModel, rf_model_name)]:
            
            print("\n\n" + name)
            model_path = Path(spark_artefacts_dir).joinpath(name)
            ml_model = model.load(str(model_path.absolute()))

            predictions = ml_model.transform(ml_df)
            predictions.persist()
            predictions.show(5)

            measures = []
            ma_eval = MulticlassClassificationEvaluator()
            for metricName in ["f1","weightedPrecision","weightedRecall","accuracy"]:
                measure = ma_eval.evaluate(predictions, {ma_eval.metricName: metricName})
                measures.append(measure)
                print(f"\t{metricName}: {measure}")

            bin_eval = BinaryClassificationEvaluator()
            for metricName in ["areaUnderROC" , "areaUnderPR"]:
                measure = bin_eval.evaluate(predictions, {bin_eval.metricName: metricName})
                measures.append(measure)
                print(f"\t{metricName}: {measure}")

            # Predicted negatives
            predicted_negative_rate = predictions.select("label").filter("label = 1.0 AND prediction = 1.0").count() / predictions.select("label").filter("label = 1.0").count()
            print(f"\tpredicted_negative_rate: {predicted_negative_rate}")
            measures.append(predicted_negative_rate)

            model_performance_lines.append([name, ml_df.count()] + measures) 

        model_performance_df = spark.createDataFrame(data= model_performance_lines, schema = MODEL_PERFORMANCE_SCHEMA)
        model_performance_df.write.jdbc(CONNECTION_STR, 'model_performance', mode = 'append', properties = CONNECTION_PROPERTIES)

def prepare_data_ml1(jenkins_builds, sonar_measures, sonar_analyses):

    ml_sonar_df = sonar_measures.join(sonar_analyses, sonar_measures.analysis_key == sonar_analyses.analysis_key, 
    how = 'inner').select(*(['revision'] + SONAR_MEASURES_NUMERICAL_COLUMNS + SONAR_MEASURES_CATEGORICAL_COLUMNS))
    df = jenkins_builds.join(ml_sonar_df, jenkins_builds.revision_number == ml_sonar_df.revision, how = 'inner')

    # Change data type from Int to Float to fit into estimators
    for column_name in ML1_NUMERICAL_COLUMNS:
        if column_name in JENKINS_BUILD_DTYPE:
            if JENKINS_BUILD_DTYPE[column_name] == 'Int64':
                df = df.withColumn(column_name, df[column_name].astype(DoubleType()))
        elif column_name in SONAR_MEASURES_DTYPE:
            if SONAR_MEASURES_DTYPE[column_name] == 'Int64':
                df = df.withColumn(column_name, df[column_name].astype(DoubleType()))
    return df

def apply_ml1(new_jenkins_builds, db_jenkins_builds, new_sonar_measures, db_sonar_measures, new_sonar_analyses, db_sonar_analyses, spark_artefacts_dir, run_mode):

    modify_result = udf(lambda x: "SUCCESS" if x == "SUCCESS" else "FAIL", StringType())
    spark.udf.register("modify_result" , modify_result)

    if new_jenkins_builds is not None:
        new_jenkins_builds = new_jenkins_builds.withColumn("result", modify_result("result"))

    if db_jenkins_builds is not None:
        db_jenkins_builds = db_jenkins_builds.withColumn("result", modify_result("result"))

    # PREPARE DATA
    if run_mode == "first":
        df = prepare_data_ml1(new_jenkins_builds, new_sonar_measures, new_sonar_analyses)

    elif run_mode == "incremental":
        # New jenkins ~ db sonar
        df1 = prepare_data_ml1(new_jenkins_builds, db_sonar_measures, db_sonar_analyses)
        # New sonar ~ db jenkins
        df2 = prepare_data_ml1(db_jenkins_builds, new_sonar_measures, db_sonar_analyses)

        df = df1.union(df2).drop_duplicates()

    df.persist()  
    print(f"DF for ML1 Count: {str(df.count())}")

    ml_df, pipeline_model = pipeline_process(df, spark_artefacts_dir, run_mode, 1)
    ml_df.persist()
    global ML1_COLUMNS
    ML1_COLUMNS = get_categorical_columns(pipeline_model.stages[2].categorySizes) + ML1_NUMERICAL_COLUMNS
    ml_df_10, ml_10_columns = feature_selector_process(ml_df, spark_artefacts_dir, run_mode, 1)
    ml_df_10.persist()

    train_predict(ml_df, spark_artefacts_dir, run_mode, 1, None)
    train_predict(ml_df_10, spark_artefacts_dir, run_mode, 1, ml_10_columns)

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

    ml_df,_ = pipeline_process(df, spark_artefacts_dir, run_mode, 2)
    ml_df.persist()
    ml_df_10, ml_10_columns = feature_selector_process(ml_df, spark_artefacts_dir, run_mode, 2)
    ml_df_10.persist()

    train_predict(ml_df, spark_artefacts_dir, run_mode, 2, None)
    train_predict(ml_df_10, spark_artefacts_dir, run_mode, 2, ml_10_columns)

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

def prepre_data_ml3(jenkins_builds, sonar_issues,sonar_analyses ,pipeline_model, label_idx_model):
    
    removed_rules_df = sonar_issues.filter("status IN ('RESOLVED', 'CLOSED', 'REVIEWED')").select("current_analysis_key","rule")
    df1 = pipeline_model.transform(removed_rules_df)
    rdd1 = df1.rdd.map(lambda x : (x[0],x[3])).reduceByKey(lambda v1,v2: sum_sparse_vectors(v1,v2)) \
                                                .map(lambda x: Row(current_analysis_key = x[0], removed_rule_vec = x[1]))
    removed_issues_rule_vec_df = spark.createDataFrame(rdd1)

    introduced_rules_df = sonar_issues.filter("status IN ('OPEN', 'REOPENED', 'CONFIRMED', 'TO_REVIEW')").select("creation_analysis_key","rule")
    df2 = pipeline_model.transform(introduced_rules_df)
    rdd2 = df2.rdd.map(lambda x : (x[0],x[3])).reduceByKey(lambda v1,v2: sum_sparse_vectors(v1,v2)) \
                                                .map(lambda x: Row(creation_analysis_key = x[0], introduced_rule_vec = x[1]))
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

        label_idx_model = StringIndexer(inputCol="result", outputCol="label").fit(new_jenkins_builds)
        label_idx_model.write().overwrite().save(str(label_idx_model_path.absolute()))

        ml_df = prepre_data_ml3(new_jenkins_builds, new_sonar_issues, new_sonar_analyses, pipeline_model, label_idx_model)

    elif run_mode == "incremental":
        
        pipeline_model = PipelineModel.load(str(pipeline_path.absolute()))
        label_idx_model = StringIndexerModel.load(str(label_idx_model_path.absolute()))

        ml_df1 = prepre_data_ml3(new_jenkins_builds, db_sonar_issues, db_sonar_analyses, pipeline_model, label_idx_model)
        ml_df2 = prepre_data_ml3(db_jenkins_builds, new_sonar_issues, db_sonar_analyses, pipeline_model, label_idx_model)

        ml_df = ml_df1.union(ml_df2)

    rules = pipeline_model.stages[0].labels
    global ML3_COLUMNS
    ML3_COLUMNS = list(map(lambda x: "removed_" + x, rules)) + list(map(lambda x: "introduced_" + x, rules))

    ml_df.persist()
    print(f"DF for ML3 Count: {ml_df.count()}")

    ml_df_10, ml_10_columns = feature_selector_process(ml_df, spark_artefacts_dir, run_mode, 3)
    ml_df_10.persist()

    train_predict(ml_df, spark_artefacts_dir, run_mode, 3, None)
    train_predict(ml_df_10, spark_artefacts_dir, run_mode, 3, ml_10_columns)

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
    apply_ml1(new_jenkins_builds, db_jenkins_builds, new_sonar_measures, db_sonar_measures, new_sonar_analyses, db_sonar_analyses, spark_artefacts_dir, run_mode)
    apply_ml2(new_jenkins_builds, db_jenkins_builds, new_sonar_issues, db_sonar_issues,  new_sonar_analyses, db_sonar_analyses, spark_artefacts_dir, run_mode)
    apply_ml3(new_jenkins_builds, db_jenkins_builds, new_sonar_issues, db_sonar_issues,  new_sonar_analyses, db_sonar_analyses, spark_artefacts_dir, run_mode)

if __name__ == "__main__":

    jenkins_data_directory = "./jenkins_data/data"
    sonar_data_directory = "./sonarcloud_data/data"
    spark_artefacts_dir = "./spark_artefacts"

    # modes = ["first", "incremental", "update_models"]

    mode = "incremental"
    then = time.time()

    print(f"Start Spark processing - mode [{mode.upper()}]")

    run(jenkins_data_directory, sonar_data_directory, spark_artefacts_dir, mode)

    spark.stop()
    print(f"Time elapsed: {time.time() - then}")

