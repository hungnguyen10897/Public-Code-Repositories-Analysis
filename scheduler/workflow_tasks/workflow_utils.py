import sys, os, configparser
from pathlib import Path

assert "PRA_HOME" in os.environ
assert os.environ["PRA_HOME"] in sys.path

from utils import CONNECTION_OBJECT

JENKINS_BUILD_DTYPE = {
    "server" : "object",
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
}

SONAR_ANALYSES_DTYPE = {
    "organization" : "object",
    "project" : "object", 
    "analysis_key" : "object", 
    "date" : "object", 
    "project_version" : "object", 
    "revision" : "object"
}

SONAR_MEASURES_DTYPE = {
    'organization' : 'object',
    'project': 'object',
    'analysis_key': 'object',
    'complexity': 'Int64',
    'class_complexity': 'float64',
    'function_complexity': 'float64',
    'file_complexity': 'float64',
    'function_complexity_distribution': 'object',
    'file_complexity_distribution': 'object',
    'complexity_in_classes': 'Int64',
    'complexity_in_functions': 'Int64',
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
    'new_coverage': 'float64',
    'new_lines_to_cover': 'Int64',
    'new_uncovered_lines': 'Int64',
    'new_line_coverage': 'float64',
    'new_conditions_to_cover': 'Int64',
    'new_uncovered_conditions': 'Int64',
    'new_branch_coverage': 'float64',
    'executable_lines_data': 'object',
    'public_api': 'Int64',
    'public_documented_api_density': 'float64',
    'public_undocumented_api': 'Int64',
    'duplicated_lines': 'Int64',
    'duplicated_lines_density': 'float64',
    'duplicated_blocks': 'Int64',
    'duplicated_files': 'Int64',
    'duplications_data': 'object',
    'new_duplicated_lines': 'Int64',
    'new_duplicated_blocks': 'Int64',
    'new_duplicated_lines_density': 'float64',
    'quality_profiles': 'object',
    'quality_gate_details': 'object',
    'violations': 'Int64',
    'blocker_violations': 'Int64',
    'critical_violations': 'Int64',
    'major_violations': 'Int64',
    'minor_violations': 'Int64',
    'info_violations': 'Int64',
    'new_violations': 'Int64',
    'new_blocker_violations': 'Int64',
    'new_critical_violations': 'Int64',
    'new_major_violations': 'Int64',
    'new_minor_violations': 'Int64',
    'new_info_violations': 'Int64',
    'false_positive_issues': 'Int64',
    'open_issues': 'Int64',
    'reopened_issues': 'Int64',
    'confirmed_issues': 'Int64',
    'wont_fix_issues': 'Int64',
    'sqale_index': 'Int64',
    'sqale_rating': 'float64',
    'development_cost': 'float64',
    'new_technical_debt': 'Int64',
    'sqale_debt_ratio': 'float64',
    'new_sqale_debt_ratio': 'float64',
    'code_smells': 'Int64',
    'new_code_smells': 'Int64',
    'effort_to_reach_maintainability_rating_a': 'Int64',
    'new_maintainability_rating': 'float64',
    'new_development_cost': 'float64',
    'alert_status': 'object',
    'bugs': 'Int64',
    'new_bugs': 'Int64',
    'reliability_remediation_effort': 'Int64',
    'new_reliability_remediation_effort': 'Int64',
    'reliability_rating': 'float64',
    'new_reliability_rating': 'float64',
    'last_commit_date': 'object',
    'vulnerabilities': 'Int64',
    'new_vulnerabilities': 'Int64',
    'security_remediation_effort': 'Int64',
    'new_security_remediation_effort': 'Int64',
    'security_rating': 'float64',
    'new_security_rating': 'float64',
    'security_hotspots': 'Int64',
    'new_security_hotspots': 'Int64',
    'security_review_rating': 'float64',
    'classes': 'Int64',
    'ncloc': 'Int64',
    'functions': 'Int64',
    'comment_lines': 'Int64',
    'comment_lines_density': 'float64',
    'files': 'Int64',
    'directories': 'Int64',
    'lines': 'Int64',
    'statements': 'Int64',
    'generated_lines': 'Int64',
    'generated_ncloc': 'Int64',
    'ncloc_data': 'object',
    'comment_lines_data': 'object',
    'projects': 'Int64',
    'ncloc_language_distribution': 'object',
    'new_lines': 'Int64'
}

SONAR_ISSUES_DTYPE = {
    "organization" : "object",
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
    "close_date" :  "object"
}

def iter_data_directory(data_dir):

    sonar_data_path = Path(data_dir).joinpath('sonarcloud')
    jenkins_data_path = Path(data_dir).joinpath('jenkins')
    assert(sonar_data_path.is_dir())
    assert(jenkins_data_path.is_dir())

    dirs = []
    dtype_dicts = []

    for organization_dir in sonar_data_path.iterdir():
        assert(organization_dir.is_dir())        
        org_analyses_dir = organization_dir.joinpath("analyses")
        org_measures_dir = organization_dir.joinpath("measures")
        org_issues_dir = organization_dir.joinpath("issues")

        dirs +=  [org_analyses_dir, org_measures_dir, org_issues_dir]
        dtype_dicts +=  [SONAR_ANALYSES_DTYPE, SONAR_MEASURES_DTYPE, SONAR_ISSUES_DTYPE]

    for server_dir in jenkins_data_path.iterdir():
        assert(server_dir.is_dir())
        server_builds_dir = server_dir.joinpath("builds")

        dirs += [server_builds_dir]
        dtype_dicts += [JENKINS_BUILD_DTYPE]

    return dirs, dtype_dicts

CONNECTION_STR = f"postgresql+psycopg2://{CONNECTION_OBJECT['user']}:{CONNECTION_OBJECT['password']}@{CONNECTION_OBJECT['host']}/{CONNECTION_OBJECT['database']}"
