# pyspark --driver-class-path postgresql-42.2.12.jar
# spark-submit --driver-class-path postgresql-42.2.12.jar spark.py

from pyspark import SparkContext, SparkConf
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import *
from pyspark.sql.utils import AnalysisException
from pyspark.sql.functions import col
from pyspark.ml.feature import OneHotEncoderEstimator, StringIndexer, VectorAssembler, MinMaxScaler, Imputer
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression, LogisticRegressionModel, DecisionTreeClassifier, DecisionTreeClassificationModel, RandomForestClassifier, RandomForestClassificationModel
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

from pathlib import Path
from collections import OrderedDict
import time

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
    "analysis_key" : "object",
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

NUMERICAL_COLUMNS = JENKINS_BUILDS_NUMERICAL_COLUMNS + SONAR_MEASURES_NUMERICAL_COLUMNS

JENKINS_BUILDS_CATEGORICAL_COLUMNS = []

SONAR_MEASURES_CATEGORICAL_COLUMNS = [
    "alert_status"
]

CATEGORICAL_COLUMNS = JENKINS_BUILDS_CATEGORICAL_COLUMNS + SONAR_MEASURES_CATEGORICAL_COLUMNS

CONNECTION_STR = "jdbc:postgresql://127.0.0.1:5432/pra"
CONNECTION_PROPERTIES = {"user": "pra", "password": "pra"}

conf = SparkConf().setMaster('local[*]')
sc = SparkContext
spark = SparkSession.builder.config(conf = conf).getOrCreate()

def get_source_data(source ,data_directory, load):

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

    imputer = Imputer(inputCols=NUMERICAL_COLUMNS , outputCols=NUMERICAL_COLUMNS )
    stages.append(imputer)

    ohe_input_cols = []
    ohe_output_cols = []
    for categorical_column in CATEGORICAL_COLUMNS:
        str_indexer = StringIndexer(inputCol=categorical_column, outputCol=categorical_column + "_index", handleInvalid='keep')
        ohe_input_cols.append(str_indexer.getOutputCol())
        ohe_output_cols.append(categorical_column + "_class_vec")
        stages.append(str_indexer)

    encoder = OneHotEncoderEstimator(inputCols=ohe_input_cols, outputCols=ohe_output_cols, handleInvalid="keep")
    stages.append(encoder)

    numerical_vector_assembler = VectorAssembler(inputCols=NUMERICAL_COLUMNS , outputCol="numerial_cols_vec", handleInvalid="keep")
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

def apply_ml1(df, spark_artefacts_dir, run_mode):

    # Change data type from Int to Float to fit into estimators
    for column_name in NUMERICAL_COLUMNS:
        if column_name in JENKINS_BUILD_DTYPE:
            if JENKINS_BUILD_DTYPE[column_name] == 'Int64':
                df = df.withColumn(column_name, df[column_name].astype(DoubleType()))
        elif column_name in SONAR_MEASURES_DTYPE:
            if SONAR_MEASURES_DTYPE[column_name] == 'Int64':
                df = df.withColumn(column_name, df[column_name].astype(DoubleType()))

    pipeline_path = Path(spark_artefacts_dir).joinpath("pipeline")
    if run_mode == "first":
        pipeline = get_ml1_pipeline()
        pipeline_model = pipeline.fit(df)
        pipeline_model.write().overwrite().save(str(pipeline_path.absolute()))

        ml_df = pipeline_model.transform(df).select('features','label')

        train,test = ml_df.randomSplit([0.7, 0.3])
        train.persist()
        test.persist()
        print("Training Dataset Count: " + str(train.count()))
        print("Test Dataset Count: " + str(test.count()))

        lr = LogisticRegression(featuresCol='features', labelCol='label', maxIter=10)
        dt = DecisionTreeClassifier(featuresCol='features', labelCol='label', maxDepth=5)
        rf = RandomForestClassifier(featuresCol = 'features', labelCol = 'label', numTrees=100)

        for algo, model_name in [(lr,"LogisticRegressionModel"),(dt,"DecisionTreeModel"),(rf,"RandomForestModel")]:

            print(f"{str(model_name)}")
            model = algo.fit(train)
            model_path = Path(spark_artefacts_dir).joinpath(model_name)
            model.write().overwrite().save(str(model_path.absolute()))
            
            predictions = model.transform(test)
            predictions.cache()
            evaluator = MulticlassClassificationEvaluator()
            for metricName in ["f1","weightedPrecision","weightedRecall","accuracy"]:
                print(f"\t{metricName}: {evaluator.evaluate(predictions, {evaluator.metricName: metricName})}")

    elif run_mode == "incremental":
        pipeline = Pipeline.load(pipeline_path)
        ml_df = pipeline.transform(df).select('features','label')
    
        for model, name in [(LogisticRegressionModel, "LogisticRegressionModel"), (DecisionTreeClassificationModel, "DecisionTreeModel"), (RandomForestClassificationModel, "RandomForestModel")]:
            model_path = Path(spark_artefacts_dir).joinpath(name)
            ml_model = model.load(str(model_path.absolute()))

            predictions = ml_model.transform(ml_df)
            predictions.cache()
            evaluator = MulticlassClassificationEvaluator()
            for metricName in ["f1","weightedPrecision","weightedRecall","accuracy"]:
                print(f"\t{metricName}: {evaluator.evaluate(predictions, {evaluator.metricName: metricName})}")

def run(jenkins_data_directory, sonar_data_directory, spark_artefacts_dir, run_mode):

    if run_mode == "incremental":
        for obj in ["pipeline","LogisticRegressionModel","DecisionTreeModel","RandomForestModel"]:
            obj_path = Path(spark_artefacts_dir).joinpath(obj)
            if not obj_path.exists():
                print(f"{obj} does not exist in spark_artefacts. Rerun with run_mode = first")
                run(jenkins_data_directory, sonar_data_directory, spark_artefacts_dir, "first")

    jenkins_builds_df = get_source_data("jenkins builds",jenkins_data_directory, run_mode)
    jenkins_builds_df = jenkins_builds_df.filter("job IS NOT NULL")
    jenkins_builds_df.persist()
    print("Jenkins builds Count: ", jenkins_builds_df.count())

    sonar_analyses_df = get_source_data("sonar analyses", sonar_data_directory, run_mode)
    sonar_analyses_df = sonar_analyses_df.filter("project IS NOT NULL")
    sonar_analyses_df.persist()
    print("Sonar analyses Count: ", sonar_analyses_df.count())

    sonar_measures_df = get_source_data("sonar measures", sonar_data_directory, run_mode)
    sonar_measures_df = sonar_measures_df.filter("project IS NOT NULL")
    sonar_measures_df = sonar_measures_df.drop(*TO_DROP_SONAR_MEASURES_COLUMNS)
    sonar_measures_df.persist()
    print("Sonar measures Count: ", sonar_measures_df.count())

    ml1_sonar_df = sonar_measures_df.join(sonar_analyses_df, sonar_measures_df.analysis_key == sonar_analyses_df.analysis_key, 
        how = 'inner').select(*(['revision'] + SONAR_MEASURES_NUMERICAL_COLUMNS + SONAR_MEASURES_CATEGORICAL_COLUMNS))

    sonar_issues_df = get_source_data("sonar issues", sonar_data_directory, run_mode)
    sonar_issues_df = sonar_issues_df.filter("project IS NOT NULL")
    sonar_issues_df.persist()
    print("Sonar issues Count: ", sonar_issues_df.count())

    # write_mode = "overwrite" if run_mode == "first" else "append"
    # jenkins_builds_df.write.jdbc(CONNECTION_STR, table="jenkins_builds", mode = write_mode, properties=CONNECTION_PROPERTIES)
    # sonar_measures_df.write.jdbc(CONNECTION_STR, table="sonar_measures", mode = write_mode, properties=CONNECTION_PROPERTIES)
    # sonar_analyses_df.write.jdbc(CONNECTION_STR, table="sonar_analyses", mode = write_mode, properties=CONNECTION_PROPERTIES)
    # sonar_issues_df.write.jdbc(CONNECTION_STR, table="sonar_issues", mode = write_mode, properties=CONNECTION_PROPERTIES)

    ml1_df = jenkins_builds_df.join(ml1_sonar_df, jenkins_builds_df.revision_number == ml1_sonar_df.revision, how = 'inner')
    ml1_df.collect()
    ml1_df.cache()  
    print("ml1_df Count: ",ml1_df.count())

    apply_ml1(ml1_df, spark_artefacts_dir, run_mode)

if __name__ == "__main__":

    jenkins_data_directory = "./jenkins_data/data"
    sonar_data_directory = "./sonarcloud_data/data"
    spark_artefacts_dir = "./spark_artefacts"

    mode = "first"
    then = time.time()
    print(f"Start Spark processing - mode: {mode.upper()}")

    run(jenkins_data_directory, sonar_data_directory, spark_artefacts_dir, "first")
    # run(jenkins_data_directory, sonar_data_directory, spark_artefacts_dir, "incremental")

    spark.stop()
    print(f"Time elapsed: {time.time() - then}")
