import sys
import jenkins
from jenkins import JenkinsException
import re
import configparser
import csv
from pathlib import Path
from datetime import datetime, timedelta
import time
import argparse
import pandas as pd
from collections import OrderedDict

BUILD_DATA_COLUMNS = [
    "job",
    "build_number",
    "result",
    "duration",
    "estimated_duration",
    "revision_number",
    "commit_id",
    "commit_ts",
    "test_pass_count",
    "test_fail_count",
    "test_skip_count",
    "total_test_duration"]

TEST_DATA_COLUMNS = [
    "job",
    "build_number",
    "package",
    "class",
    "name",
    "duration",
    "status"]

def get_projects(path):
    projects = []
    with open(path, 'r') as f:
        for line in f:
            project_name = line.strip().strip("\"")
            projects.append(project_name)

    return projects

def extract_test_data(test_report, job_name, build_number, build_only):

    if not test_report:
        return (None,[])

    test_duration = test_report['duration']

    if build_only:
        return (test_duration, [])

    # Get test specific data.
    test_cases_result = []

    #Each suite test a class
    for suite in test_report['suites']:

        class_structure = suite['name'].split('.')
        package = ".".join(class_structure[:-1])
        class_ = class_structure[-1]

        # Each case tests something different in a class
        for case in suite['cases']:
        
            test_cases_result.append((job_name, build_number, package, class_,case['name'],\
            case['duration'], case['status']))
    
    return (test_duration, test_cases_result)

def process_date_time(time_str):
    if time_str is None:
        return None

    ts_parts = time_str.split(' ')
    if len(ts_parts) != 3:
        ts = datetime.strptime(time_str[:19], "%Y-%m-%dT%H:%M:%S")
        return ts

    tz = ts_parts[2]
    
    ts = datetime.strptime(" ".join(ts_parts[:2]), "%Y-%m-%d %H:%M:%S")
    offset = timedelta(hours = int(tz[1:3]), minutes = int(tz[3:5]))

    if tz[0] == '-':
        ts = ts + offset
    elif tz[0] == '+':
        ts = ts - offset
    
    return ts

def get_data(builds, job_name , server, build_only):
    
    builds_data = []
    tests_data = []

    for build in builds:

        if 'id' in build:
            build_number = int(build['id'])
        else:
            if 'number' in build:
                build_number = int(build['number'])
            else:
                continue

        build_result = None if 'result' not in build else build['result']
        build_duration = None if 'duration' not in build else build['duration']
        build_estimated_duration = None if 'estimatedDuration' not in build else build['estimatedDuration']

        # Get revision number
        revision_number = None

        if 'actions' in build:
            for action in build['actions']:
                if not action:
                    continue

                if action['_class'].split('.')[-1] == 'TestResultAction':
                    fail_count = action['failCount']
                    skip_count = action['skipCount']
                    total_count = action['totalCount']

                if action['_class'].split('.')[-1] == 'BuildData':
                    if 'lastBuiltRevision' in action:
                        if 'SHA1' in action['lastBuiltRevision']:
                            revision_number = action['lastBuiltRevision']['SHA1']

        # Get info about commit
        commit_ids_ts = {}

        if 'changeSet' in build and 'items' in build['changeSet']:
            changeset_items = build['changeSet']['items']
            for item in changeset_items:
                if 'commitId' in item:
                    if 'date' in item:
                        commit_ids_ts[item['commitId']] = process_date_time(item['date'])
                    else:
                        commit_ids_ts[item['commitId']] = None

            # if len(commit_ids_ts) != 1:
            #     print(f"WARNING: {len(commit_ids_ts)} commits ids found for the build \
            #         {build['fullDisplayName']}")

        # Get general test data:
        test_report = server.get_build_test_report(job_name , build_number, depth=0)
        test_result = []

        if not test_report:

            print(f"WARNING: No test report for {job_name} - {str(build_number)}")
            build_fail_count = None
            build_pass_count = None
            build_skip_count = None
            build_total_test_duration = None
        
        else:

            build_fail_count = test_report['failCount']
            build_skip_count = test_report['skipCount']

            if 'passCount' in test_report:
                build_pass_count = test_report['passCount']

            if 'totalCount' in test_report:
                build_pass_count = test_report['totalCount'] - build_fail_count - build_skip_count

            # Get specific test data:
            build_total_test_duration = None
            test_report_class = test_report['_class'].split('.')[-1]
            if test_report_class == "SurefireAggregatedReport":

                for child_report in test_report['childReports']:
                    duration, child_test_result = extract_test_data(child_report['result'], job_name, build_number, build_only)
                    if duration is not None:
                        if build_total_test_duration is None:
                            build_total_test_duration = duration
                        else:
                            build_total_test_duration += duration

                    test_result = test_result + child_test_result

            elif test_report_class == "TestResult":
                build_total_test_duration, test_result = extract_test_data(test_report, job_name, build_number, build_only)

            else:
                print(f"WARNING: Unrecognized test report class for {job_name} - {str(build_number)}")
        
        # There can be multiple lines for a build of a job due to multiple commits
        if len(commit_ids_ts) > 0:
            for commit_id, ts in commit_ids_ts.items():
                build_data = (job_name, build_number, build_result, build_duration, build_estimated_duration, \
                    revision_number, commit_id, ts, build_pass_count, build_fail_count, build_skip_count, build_total_test_duration)
        
        else:
            build_data = (job_name, build_number, build_result, build_duration, build_estimated_duration, \
                    revision_number, None, None, build_pass_count, build_fail_count, build_skip_count, build_total_test_duration)
        
        builds_data.append(build_data)
        tests_data = tests_data + test_result

    return builds_data, tests_data

def write_to_file(job_data, output_dir_str, build_only):
    
    job_name, df_builds, df_tests = job_data
    output_dir = Path(output_dir_str)

    if not build_only and df_tests is not None:
        tests_dir = output_dir.joinpath('tests')
        tests_dir.mkdir(parents=True, exist_ok=True)
        output_file_tests = tests_dir.joinpath(f"{job_name.lower().replace(' ', '_')}_tests.csv")
        df_tests.to_csv(path_or_buf = output_file_tests, index=False, header=True)

    if df_builds is not None:
        builds_dir = output_dir.joinpath('builds')
        builds_dir.mkdir(parents=True, exist_ok=True)
        output_file_builds = builds_dir.joinpath(f"{job_name.lower().replace(' ', '_')}_builds.csv")
        df_builds.to_csv(path_or_buf = output_file_builds, index=False, header = True)

def get_jobs_info(project, server, first_load, sub_project=False):
    # first_load: True if need to take all builds

    jobs_info = []

    if sub_project:
        jobs = [server.get_job_info(project, depth= 0, fetch_all_builds = first_load)]

    # Not a sub_project
    else:

        regex = re.compile(f"^.*{project}.*$", re.IGNORECASE)
        jobs = server.get_job_info_regex(regex, folder_depth=0, depth=0)
        
    for job_info in jobs:

        class_ = job_info['_class'].split('.')[-1]
        if class_ in ['Folder', 'OrganizationFolder', 'WorkflowMultiBranchProject']:
            for sub_job in job_info['jobs']:
                fullName = sub_job['fullName']
                jobs_info += get_jobs_info(fullName, server, first_load, True)

        else:
            # number of builds = 100 means there may be more builds to be fetched => fetched again!
            if first_load and len(job_info['builds']) == 100 and not sub_project:
                jobs_info.append(server.get_job_info(job_info['fullName'], depth= 0, fetch_all_builds = True))

            else:
                jobs_info.append(job_info)

    return jobs_info

def project_job(project_name, server, first_load=False, output_dir_str ='./data', build_only = False):

    for job_info in get_jobs_info(project_name, server, first_load):

        fullName = job_info['fullName']

        # 'scm' field checking
        if 'scm' in job_info and '_class' in job_info['scm']:
            scm_class = job_info['scm']['_class'].split('.')[-1]
            if scm_class in ['SubversionSCM','NullSCM']:
                continue

        builds = []
        #get builds info:
        for build in job_info['builds']:
            build_number = build['number']
            try:
                build_data = server.get_build_info(fullName, build_number, depth=1)
                builds.append(build_data)
            except JenkinsException as e:
                print(f"JenkinsException: {e}")

        # job_data contains both build and test data
        builds_data, tests_data = get_data(builds, fullName, server, build_only)

        df_builds = None
        if builds_data != []:
            df_builds = pd.DataFrame(data = builds_data, columns=BUILD_DATA_COLUMNS)

        df_tests = None    
        if tests_data != []:
            df_tests = pd.DataFrame(data = tests_data, columns=TEST_DATA_COLUMNS)
       
        write_to_file((fullName,df_builds, df_tests), output_dir_str, build_only)

if __name__ == "__main__":

    ap = argparse.ArgumentParser(description="Scrip to fetch data from Apache Jenkins Server at https://builds.apache.org/")

    ap.add_argument("-f","--format", choices=['csv', 'parquet'], default='csv', help="Output file format, either csv or parquet")
    ap.add_argument("-o","--output-path", default='./data4' , help="Path to output file directory, default is './data'")
    ap.add_argument("-l", "--load", choices=['first_load', 'incremental_load'], default='first_load', help="First load or incremental load")
    ap.add_argument("-b","--build-only",  help = "Write only build data.", action='store_true')
    ap.add_argument("-p","--projects", default = './projects_test.csv', help = "Path to a file containing names of all projects to load")
    ap.add_argument("-a","--all", action="store_true", help = "Load data from all projects available on the server, this will ignore -p argument")

    args = vars(ap.parse_args())

    build_only = args['build_only']
    output_path = args['output_path']
    projects_path = args['projects']
    all = args['all']

    if (projects_path is None and all is False) or (projects_path is not None and all is True):
        print("Provide either -a to load build data of all projects from Jenkins server or -p with a file containg project's names")
        sys.exit(1)


    start = time.time()
    server = jenkins.Jenkins('https://builds.apache.org/')

    # Sometimes connecting to Jenkins server is banned due to ill use of API
    # Test connection to server
    print(f"Jenkins-API version: {server.get_version()}")

    if not all:
        print("Processing projects.")
        project_names = get_projects(projects_path)    
        print(project_names)

        for project_name in project_names:
            print(f"Project: {project_name}")
            project_job(project_name, server, first_load = True, output_dir_str = output_path, build_only= build_only)
    
    else:
        print("Processing all jobs.")
        pass
    
    end = time.time()
    print(f"Time total: {end-start}")