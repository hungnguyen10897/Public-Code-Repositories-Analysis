import sys, os, psycopg2
from collections import OrderedDict
from pyspark.sql.types import *

assert "PRA_HOME" in os.environ
sys.path.insert(1, os.environ["PRA_HOME"])

from utils import CONNECTION_OBJECT

CONNECTION_STR = f"jdbc:postgresql://{CONNECTION_OBJECT['host']}/{CONNECTION_OBJECT['database']}"
CONNECTION_PROPERTIES = {"user": f"{CONNECTION_OBJECT['user']}", "password": f"{CONNECTION_OBJECT['password']}"}

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

TOP_ISSUE_SCHEMA = StructType([
    StructField("organization", StringType()),
    StructField("project", StringType()),
    StructField("model", StringType()),
    StructField("input_data_amount", IntegerType()),
    StructField("issue_1", StringType()),
    StructField("issue_importance_1", FloatType()),
    StructField("issue_2", StringType()),
    StructField("issue_importance_2", FloatType()),
    StructField("issue_3", StringType()),
    StructField("issue_importance_3", FloatType()),
    StructField("issue_4", StringType()),
    StructField("issue_importance_4", FloatType()),
    StructField("issue_5", StringType()),
    StructField("issue_importance_5", FloatType()),
    StructField("issue_6", StringType()),
    StructField("issue_importance_6", FloatType()),
    StructField("issue_7", StringType()),
    StructField("issue_importance_7", FloatType()),
    StructField("issue_8", StringType()),
    StructField("issue_importance_8", FloatType()),
    StructField("issue_9", StringType()),
    StructField("issue_importance_9", FloatType()),
    StructField("issue_10", StringType()),
    StructField("issue_importance_10", FloatType()),
])

def get_batches(connection_object):
    conn = psycopg2.connect(
        host=connection_object['host'],
        database=connection_object['database'],
        user=connection_object['user'],
        password=connection_object['password'])

    cursor = conn.cursor()

    cursor.execute("SELECT MAX(batch_number) FROM source")
    max_batch_num = cursor.fetchone()[0]

    if max_batch_num is None:
        # No batch
        return []
    else:
        batches = []
        org_keys = []
        servers = []
        # Per batch
        for i in range(max_batch_num + 1):
            cursor.execute(f"SELECT sonar_org_key, jenkins_server FROM source WHERE batch_number = {i}")
            for e in cursor.fetchall():
                org_keys.append(e[0])
                servers.append(e[1])
            batches.append((i, org_keys, servers))
        return batches

def get_data_from_db(spark, table, processed, custom_filter=[], org_server_filter_elements=None, all_columns=False):

    if table == "jenkins_builds":
        dtype = JENKINS_BUILD_DTYPE
        org_server_filter = "server"
    elif table == "sonar_analyses":
        dtype = SONAR_ANALYSES_DTYPE
        org_server_filter = "organization"
    elif table == "sonar_issues":
        dtype = SONAR_ISSUES_DTYPE
        org_server_filter = "organization"
    elif table == "sonar_measures":
        dtype = SONAR_MEASURES_DTYPE
        org_server_filter = "organization"

    if all_columns:
        columns = "*"
    else:    
        columns = ", ".join(dtype.keys())
    
    filters = []

    # Filter accumulation
    if org_server_filter_elements is not None:
        filters.append(f"""
            {org_server_filter} IN ({"'" + "', '".join(org_server_filter_elements) + "'"})
            """)

    if processed is not None:
        filters.append(f"processed is {processed}")

    filters += custom_filter

    filter_clause = "" if not filters else "WHERE " + " AND ".join(filters)
        
    query = f"SELECT {columns} FROM {table} " + filter_clause

    df = spark.read \
        .format("jdbc") \
        .option("url", CONNECTION_STR) \
        .option("user", CONNECTION_PROPERTIES["user"]) \
        .option("password", CONNECTION_PROPERTIES["password"]) \
        .option("query", query)\
        .load()

    return df
