# pyspark --conf spark.executor.extraClassPath=sqlite-jdbc-3.30.1.jar --driver-class-path sqlite-jdbc-3.30.1.jar --jars sqlite-jdbc-3.30.1.jar
# spark-submit --conf spark.executor.extraClassPath=sqlite-jdbc-3.30.1.jar --driver-class-path sqlite-jdbc-3.30.1.jar --jars sqlite-jdbc-3.30.1.jar spark.py
from pyspark.sql import SparkSession
from pathlib import Path
from pyspark.sql import DataFrame
from pyspark.sql.types import *
from pyspark import SparkContext, SparkConf
from collections import OrderedDict
from functools import reduce

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
    "total_test_duration" : "float64"})

JENKINS_TEST_DTYPE = OrderedDict({
    "job" : "object",
    "build_number" : "Int64",
    "package" : "object",
    "class" : "object",
    "name" : "object",
    "duration" : "float64",
    "status" : "object"})

SONAR_DTYPE = OrderedDict({
    'project': 'object',
    'version': 'object',
    'date' : 'object',
    'revision': 'object',
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
    'test_execution_time': 'object',
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
    'development_cost': 'object',
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

sc = SparkContext()
spark = SparkSession.builder.getOrCreate()

def retrieve_sqlite(sqlite_conn_url):
    db = {}

    projects_properties = {}
    projects_properties['customSchema'] = "projectID STRING, gitLink STRING, jiraLink STRING, sonarProjectKey STRING"
    projects_df = spark.read.jdbc(sqlite_conn_url, 'PROJECTS', properties = projects_properties)
    db['PROJECTS'] = projects_df

    refactoring_miner_properties = {}
    refactoring_miner_properties['customSchema'] = "projectID STRING, commitHash STRING, refactoringType STRING, refactoringDetail STRING"
    refactoring_miner_df = spark.read.jdbc(sqlite_conn_url, 'REFACTORING_MINER', properties = refactoring_miner_properties)
    db['REFACTORING_MINER'] = refactoring_miner_df

    return db

def refactor_type_count(refactoring_miner_df):

    rows = refactoring_miner_df.select("refactoringType").distinct().collect()
    select_statement = []
    for row in rows:
        type = row.refactoringType
        comparison_str = "\'" + row.refactoringType + "\'"
        field_name = type.replace(' ','_') + "_Count"
        sum_str = f"SUM(case refactoringType when {comparison_str} then 1 else 0 end) AS {field_name}"
        select_statement.append(sum_str)
    
    sql_str = f"""
        SELECT 
            projectID,
            commitHash,
            {",".join(select_statement)}
        FROM REFACTORING_MINER GROUP BY projectID,commitHash
    """

    return spark.sql(sql_str)

def get_jenkins_builds_data(jenkins_data_directory):
    
    data_path = Path(jenkins_data_directory)
    builds_path = data_path.joinpath('builds')

    field = []
    for col,type in JENKINS_BUILD_DTYPE.items():
        if col == 'commit_ts':
            field.append(StructField(col, TimestampType(), True))
        elif type == 'object':
            field.append(StructField(col, StringType(), True))
        elif type == 'Int64':
            field.append(StructField(col, IntegerType(), True))
        elif type == 'float64':
            field.append(StructField(col, DoubleType(), True))

    schema = StructType(field)
    df = spark.read.csv(str(builds_path.absolute()) + '/*.csv', sep=',', schema = schema, ignoreLeadingWhiteSpace = True, 
        ignoreTrailingWhiteSpace = True, header=True)
    return df


def get_sonar_data(sonar_data_directory):

    path = Path(sonar_data_directory)
    csv_path = path.joinpath("csv")

    field = []
    for col,type in SONAR_DTYPE.items():
        if col == 'date':
            field.append(StructField(col, TimestampType(), True))
        elif type == 'object':
            field.append(StructField(col, StringType(), True))
        elif type == 'Int64':
            field.append(StructField(col, IntegerType(), True))
        elif type == 'float64':
            field.append(StructField(col, FloatType(), True))

    schema = StructType(field)
    df = spark.read.csv(str(csv_path.absolute())+ "/*_staging.csv", sep=',', schema = schema, ignoreLeadingWhiteSpace = True, ignoreTrailingWhiteSpace = True, header=True, mode = 'FAILFAST')
    return df

if __name__ == "__main__":

    jenkins_data_directory = "./jenkins_data/data"
    sonar_data_directory = "./sonarcloud_data/data"
    sqlite_conn_url = "jdbc:sqlite:/home/hung/MyWorksapce/BachelorThesis/SQLite-Database/technicalDebtDataset.db"

    # db = retrieve_sqlite(sqlite_conn_url)
    # projects_df = db['PROJECTS']
    # refactoring_miner_df = db['REFACTORING_MINER']

    # refactoring_miner_df = refactoring_miner_df.filter("refactoringType IS NOT NULL")
    # refactoring_miner_df.persist()
    # refactoring_miner_df.createOrReplaceTempView("REFACTORING_MINER")

    # refactor_count_df = refactor_type_count(refactoring_miner_df)
    # refactor_count_df.persist()
    # refactoring_miner_df.unpersist()


    jenkins_builds_df = get_jenkins_builds_data(jenkins_data_directory)
    jenkins_builds_df = jenkins_builds_df.filter("revision_number IS NOT NULL")
    jenkins_builds_df.persist()
    jenkins_builds_df.show(5)
    #jenkins_builds_df.repartition(1).write.csv('./jenkins_builds.csv', header=True)
    print("Jenkins Count: ", jenkins_builds_df.count())

    sonar_df = get_sonar_data(sonar_data_directory)
    sonar_df = sonar_df.filter("revision IS NOT NULL")
    sonar_df.persist()
    sonar_df.collect()
    sonar_df.show(5)
    print("Sonar Count: ", sonar_df.count())

    result = jenkins_builds_df.join(sonar_df, jenkins_builds_df.revision_number == sonar_df.revision, how = 'inner')
    result.cache()
    
    print("Result Count: ",result.count())

    spark.stop()