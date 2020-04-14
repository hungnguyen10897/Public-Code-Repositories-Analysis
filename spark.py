# pyspark --driver-class-path postgresql-42.2.12.jar
# spark-submit --driver-class-path postgresql-42.2.12.jar spark.py
from pyspark.sql import SparkSession
from pathlib import Path
from pyspark.sql import DataFrame
from pyspark.sql.types import *
from pyspark.sql.utils import AnalysisException

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

# Drop these columns due to too many nulls
TO_DROP_SONAR_COLUMNS = [
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

TRAINING_COLUMNS = [
    "complexity"
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
    "sqale_rating"
    "development_cost",
    "sqale_debt_ratio",
    "new_sqale_debt_ratio",
    "code_smells",
    "effort_to_reach_maintainability_rating_a",
    "new_development_cost",
    "alert_status",
    'bugs'
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

CATEGORICAL_COLUMNS = [

    "alert_status"
]

conf = SparkConf().setMaster('local[*]')
sc = SparkContext
spark = SparkSession.builder.config(conf = conf).getOrCreate()

def get_source_data(source ,data_directory):

    data_path = Path(data_directory)
    if source == "jenkins builds":
        files_dir = data_path.joinpath('builds')
        DTYPE = JENKINS_BUILD_DTYPE
    elif source == "jenkins tests":
        files_dir = data_path.joinpath('tests')
        DTYPE = JENKINS_TEST_DTYPE
    elif source == "sonarqube":
        files_dir = data_path.joinpath('csv')
        DTYPE = SONAR_DTYPE

    field = []
    for col,type in DTYPE.items():
        if col in ['date', 'last_commit_date', 'commit_ts']:
            field.append(StructField(col, TimestampType(), True))
        elif type == 'object':
            field.append(StructField(col, StringType(), True))
        elif type == 'Int64':
            field.append(StructField(col, IntegerType(), True))
        elif type == 'float64':
            field.append(StructField(col, DoubleType(), True))

    schema = StructType(field)
    print(str(files_dir.absolute()))
    try:
        df = spark.read.csv(str(files_dir.absolute()) + '/*_staging.csv', sep=',', schema = schema, ignoreLeadingWhiteSpace = True, 
            ignoreTrailingWhiteSpace = True, header=True, mode = 'FAILFAST')
    except AnalysisException:
        print(f"No _staging csv for [{source}]")
        df = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)
    return df

def first_ml_train():
    pass

if __name__ == "__main__":

    jenkins_data_directory = "./jenkins_data/data"
    sonar_data_directory = "./sonarcloud_data/data"

    jenkins_builds_df = get_source_data("jenkins builds",jenkins_data_directory)
    jenkins_builds_df = jenkins_builds_df.filter("job IS NOT NULL")
    jenkins_builds_df.persist()
    print("Jenkins Count: ", jenkins_builds_df.count())

    sonar_df = get_source_data("sonarqube", sonar_data_directory)
    sonar_df = sonar_df.filter("project IS NOT NULL")
    sonar_df = sonar_df.drop(*TO_DROP_SONAR_COLUMNS)
    sonar_df.persist()
    print("Sonar Count: ", sonar_df.count())

    url = "jdbc:postgresql://127.0.0.1:5432/pra"
    properties = {"user": "pra", "password": "pra"}

    # jenkins_builds_df.write.jdbc(url, table="jenkins_builds", mode = 'append', properties=properties)
    # sonar_df.write.jdbc(url, table="sonarqube", mode = 'append', properties=properties)

    # result = jenkins_builds_df.join(sonar_df, jenkins_builds_df.revision_number == sonar_df.revision, how = 'inner')
    # result.cache()
    
    # print("Result Count: ",result.count())

    print("FINISH!!!!!!")
