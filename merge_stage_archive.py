import pandas as pd
from pathlib import Path
import sys
import argparse

JENKINS_BUILD_DTYPE = {
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

SONAR_DTYPE = {
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

def merge(file_directory, DTYPE):

    if not file_directory.exists():
        return

    for file in file_directory.glob("*_staging.csv"):
        archive_file = Path(str(file).replace("_staging", ""))
        if archive_file.exists():

            old_df = pd.read_csv(archive_file.resolve(), dtype=DTYPE, header=0)
            new_df = pd.read_csv(file.resolve(), dtype = DTYPE, header = 0)

            df = pd.concat([new_df, old_df], ignore_index = True)
            df.drop_duplicates(inplace=True)
            
            df.to_csv(path_or_buf= archive_file, index=False, header=True)
            
            file.unlink()
        else:
            file.rename(archive_file)

def main(jenkins_data_dir, sonar_data_dir):

    jenkins_builds_dir = jenkins_data_dir.joinpath("builds")
    if not jenkins_builds_dir.exists():
        print(f"ERROR: Jenkins builds data directory does not exsists - {jenkins_builds_dir.resolve()}")
        sys.exit(1)

    jenkins_tests_dir = jenkins_data_dir.joinpath("tests")

    sonar_csv_dir = sonar_data_dir.joinpath("csv")
    if not sonar_csv_dir.exists():
        print(f"ERROR: Sonar data directory does not exsists: {sonar_csv_dir.resolve()}")
        sys.exit(1)

    dirs = [jenkins_builds_dir, jenkins_tests_dir, sonar_csv_dir]
    dtype_dicts = [JENKINS_BUILD_DTYPE, JENKINS_TEST_DTYPE, SONAR_DTYPE]

    for dir,dtype in zip(dirs, dtype_dicts):
        print(f"Merging files in directory {dir.resolve()}")
        merge(dir, dtype)

if __name__ == "__main__":
    
    ap = argparse.ArgumentParser(description="Script to merge staging and archive files.")

    ap.add_argument("-j","--jenkins", default='./sonar_data' , help="Path to jenkins data directory.")
    ap.add_argument("-s", "--sonar", default = "", help = "Path to Sonarqube data directory.")

    args = vars(ap.parse_args())

    jenkins_data_dir = Path(args['jenkins'])
    sonar_data_dir = Path(args['sonar'])

    main(jenkins_data_dir, sonar_data_dir)


