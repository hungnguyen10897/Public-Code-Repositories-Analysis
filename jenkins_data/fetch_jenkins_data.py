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

BUILD_DATA_COLUMNS = OrderedDict({
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

TEST_DATA_COLUMNS = OrderedDict({
    "job" : "object",
    "build_number" : "Int64",
    "package" : "object",
    "class" : "object",
    "name" : "object",
    "duration" : "float64",
    "status" : "object"})

def get_projects(path):
    p = Path(path)
    projects = []
    if not p.exists():
        print("Projects file path {p.resolve()} does not exist. Exiting")
        sys.exit(1)
    with open(p.resolve(), 'r') as f:
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
        build_fail_count = None
        build_pass_count = None
        build_skip_count = None

        if 'actions' in build:
            for action in build['actions']:
                if not action:
                    continue

                # Get general test data, in case server.get_build_test_report() gets error or returns None
                if action['_class'].split('.')[-1] == 'TestResultAction':
                    build_fail_count = action['failCount']
                    build_skip_count = action['skipCount']

                    if 'passCount' in action:
                        build_pass_count = action['passCount']

                    if 'totalCount' in action:
                        build_pass_count = action['totalCount'] - build_fail_count - build_skip_count

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

        # Get specific test data:
        try:
            test_report = server.get_build_test_report(job_name , build_number, depth=0)
        except JenkinsException as e:
            print(f"ERROR: Exception upon getting build test report for '{job_name}' - build number {build_number} : {e}")
            test_report = None
        test_result = []

        build_total_test_duration = None

        if test_report is not None:

            build_fail_count = test_report['failCount']
            build_skip_count = test_report['skipCount']

            if 'passCount' in test_report:
                build_pass_count = test_report['passCount']

            if 'totalCount' in test_report:
                build_pass_count = test_report['totalCount'] - build_fail_count - build_skip_count

            # Get specific test data:
            build_total_test_duration = None
            test_report_class = test_report['_class'].split('.')[-1]
            if test_report_class in ["SurefireAggregatedReport", "MatrixTestResult"]:

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

def write_to_file(job_data, old_builds_df, output_dir_str, build_only):
    
    job_name, df_builds, df_tests = job_data
    output_dir = Path(output_dir_str)

    if not build_only and df_tests is not None:
        tests_dir = output_dir.joinpath('tests')
        tests_dir.mkdir(parents=True, exist_ok=True)
        output_file_tests = tests_dir.joinpath(f"{job_name.lower().replace(' ', '_').replace('/','_')}_tests.csv")

        old_tests_df = None
        if output_file_tests.exists():
            old_tests_df = pd.read_csv(output_file_tests.resolve(), dtype=TEST_DATA_COLUMNS, header=0)
            output_file_tests.unlink()

        agg_tests_df = pd.concat([df_tests, old_tests_df], ignore_index = True)
        agg_tests_df.to_csv(path_or_buf = output_file_tests, index=False, header=True)

    if df_builds is not None:
        builds_dir = output_dir.joinpath('builds')
        builds_dir.mkdir(parents=True, exist_ok=True)
        output_file_builds = builds_dir.joinpath(f"{job_name.lower().replace(' ', '_').replace('/','_')}_builds.csv")

        if output_file_builds.exists():
            output_file_builds.unlink()

        agg_builds_df = pd.concat([df_builds, old_builds_df], ignore_index = True)
        agg_builds_df.to_csv(path_or_buf = output_file_builds, index=False, header = True)

def get_jobs_info(name, server, is_job, output_dir_str):

    jobs_info = []

    if is_job:
        jobs = [server.get_job_info(name, depth= 0, fetch_all_builds = False)]

    # Not a job but a project's name
    else:
        regex = re.compile(f"^.*{name}.*$", re.IGNORECASE)
        jobs = server.get_job_info_regex(regex, folder_depth=0, depth=0)
    
    for job_info in jobs:

        class_ = job_info['_class'].split('.')[-1]
        job_name = job_info['fullName']
        if class_ in ['Folder', 'OrganizationFolder', 'WorkflowMultiBranchProject']:
            for sub_job in job_info['jobs']:
                if 'fullName' in sub_job:
                    fullName = sub_job['fullName']
                else:
                    fullName = f"{job_name}/{sub_job['name']}"
                jobs_info += get_jobs_info(fullName, server, True, output_dir_str= output_dir_str)

        else:
            num_builds = len(job_info['builds'])          # 0 <= num_builds <= 100
            if num_builds == 0:
                continue

            diff = None
            # Get latest build on file
            df = None
            p = Path(output_dir_str).joinpath("builds").joinpath(f"{job_name.lower().replace(' ', '_').replace('/','_')}_builds.csv")
            if p.exists():
                df = pd.read_csv(p.resolve(), dtype=BUILD_DATA_COLUMNS, parse_dates=['commit_ts'], header=0)

                latest_build_on_file = df['build_number'].max()
                lastest_build_on_server = job_info['lastBuild']['number']
                diff = lastest_build_on_server - latest_build_on_file

                if diff <= 0:
                    continue
                elif diff >= 100:
                    full_job_info = server.get_job_info(job_name, depth= 0, fetch_all_builds = True)
                    jobs_info.append((full_job_info, df))
                else:
                    jobs_info.append((job_info, df))

            else:
                if num_builds == 100:
                    full_job_info = server.get_job_info(job_name, depth= 0, fetch_all_builds = True)
                    jobs_info.append((full_job_info, df))
                else:
                    jobs_info.append((job_info, df))

    return jobs_info

def get_all_job_names(server):
    jobs = server.get_all_jobs(folder_depth=2)
    job_names = []
    for job in jobs:
        class_ = job['_class'].split('.')[-1]
        if class_ in ['Folder', 'OrganizationFolder', 'WorkflowMultiBranchProject']:
            continue
        else:
            name = job['name']
            job_names.append(name)
    return job_names

def process_jobs(project_name, is_job, server, first_load, output_dir_str ='./data', build_only = False):

    for job_info, old_builds_df in get_jobs_info(project_name, server, is_job, output_dir_str= output_dir_str):

        # Get latest build number on file
        latest_build_on_file = -1
        if old_builds_df is not None:
            latest_build_on_file = old_builds_df['build_number'].max()

        fullName = job_info['fullName']
        print(f"\tJob: {fullName}")

        # # 'scm' field checking
        # if 'scm' in job_info and '_class' in job_info['scm']:
        #     scm_class = job_info['scm']['_class'].split('.')[-1]
        #     if scm_class in ['SubversionSCM','NullSCM']:
        #         print(f"SCM Class: {scm_class}")
        #         continue

        builds = []
        #get builds info:
        for build in job_info['builds']:
            build_number = build['number']
            if build_number <= latest_build_on_file:
                continue
            try:
                build_data = server.get_build_info(fullName, build_number, depth=1)
                builds.append(build_data)
            except JenkinsException as e:
                print(f"JenkinsException: {e}")

        builds_data, tests_data = get_data(builds, fullName, server, build_only)

        df_builds = None
        if builds_data != []:
            df_builds = pd.DataFrame(data = builds_data, columns=list(BUILD_DATA_COLUMNS.keys()))

        df_tests = None    
        if tests_data != []:
            df_tests = pd.DataFrame(data = tests_data, columns=list(TEST_DATA_COLUMNS.keys()))
       
        write_to_file((fullName,df_builds, df_tests),old_builds_df, output_dir_str, build_only)

if __name__ == "__main__":

    ap = argparse.ArgumentParser(description="Scrip to fetch data from Apache Jenkins Server at https://builds.apache.org/")

    ap.add_argument("-f","--format", choices=['csv', 'parquet'], default='csv', help="Output file format, either csv or parquet")
    ap.add_argument("-o","--output-path", default='./data' , help="Path to output file directory, default is './data'")
    # ap.add_argument("-l", "--load", choices=['first_load', 'incremental_load'], default='incremental_load', help="First load or incremental load, same if no data is available")
    ap.add_argument("-b","--build-only",  help = "Write only build data.", action='store_true')
    ap.add_argument("-p","--projects", default = './projects_test.csv', help = "Path to a file containing names of all projects to load")
    ap.add_argument("-a","--all", action="store_true", help = "Load data from all jobs available on the server, this will ignore -p argument")

    args = vars(ap.parse_args())

    build_only = args['build_only']
    output_path = args['output_path']
    projects_path = args['projects']
    all = args['all']

    start = time.time()
    server = jenkins.Jenkins('https://builds.apache.org/')

    # Sometimes connecting to Jenkins server is banned due to ill use of API
    # Test connection to server
    print(f"Jenkins-API version: {server.get_version()}")

    if not all:
        print("Processing projects.")
        names = get_projects(projects_path)    
        print(names)
    else:
        names = get_all_job_names(server)
        print(f"Processing all {len(names)} jobs.")

    i = 0
    for name in names:
        print(f"#{i}")
        i += 1
        process_jobs(name, all, server, True, output_dir_str = output_path, build_only= build_only)

    end = time.time()
    print(f"Time total: {end-start}")