from pathlib import Path

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
    "total_test_duration" : "float64"}

JENKINS_TEST_DTYPE = {
    "job" : "object",
    "build_number" : "Int64",
    "package" : "object",
    "class" : "object",
    "name" : "object",
    "duration" : "float64",
    "status" : "object"}

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
    'sonarjava_feedback': 'object',
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
    'new_lines': 'object'}

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
        server_tests_dir = server_dir.joinpath("tests")

        dirs += [server_builds_dir, server_tests_dir]
        dtype_dicts += [JENKINS_BUILD_DTYPE, JENKINS_TEST_DTYPE]

    return dirs, dtype_dicts