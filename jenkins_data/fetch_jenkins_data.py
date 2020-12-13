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

JENKINS_BUILD_DTYPE = OrderedDict({
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
    "total_test_duration" : "float64"})

JENKINS_TEST_DTYPE = OrderedDict({
    "job" : "object",
    "build_number" : "Int64",
    "package" : "object",
    "class" : "object",
    "name" : "object",
    "duration" : "float64",
    "status" : "object"})

def get_proper_file_name(origin):
    p = re.compile("[^0-9a-z-_]")
    return p.sub('_', origin.lower())

def get_projects(path):
    p = Path(path)
    projects = []
    if not p.exists():
        print(f"Projects file path {p.resolve()} does not exist. Exiting")
        sys.exit(1)
    with open(p.resolve(), 'r') as f:
        for line in f:
            project_name = line.strip().strip("\"")
            projects.append(project_name)

    return projects

def extract_test_data(test_report, job_name, build_number, build_only):

    if not test_report:
        return (None,[])

    test_duration = None if 'duration' not in test_report else test_report['duration']

    if build_only:
        return (test_duration, [])

    # Get test specific data.
    test_cases_result = []

    #Each suite test a class
    suites = [] if 'suites' not in test_report else test_report['suites']
    for suite in suites:

        class_structure = suite['name'].split('.')
        package = ".".join(class_structure[:-1])
        class_ = class_structure[-1]

        # Each case tests something different in a class
        for case in suite['cases']:

            name = case['name'].replace("\n","_").replace("\r","_")
        
            test_cases_result.append((job_name, build_number, package, class_,name,\
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

def get_data(builds, job_name , server, server_url, build_only):
    
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
                build_data = (server_url, job_name, build_number, build_result, build_duration, build_estimated_duration, \
                    revision_number, commit_id, ts, build_pass_count, build_fail_count, build_skip_count, build_total_test_duration)
        
        else:
            build_data = (server_url, job_name,  build_number, build_result, build_duration, build_estimated_duration, \
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
        output_file_tests = tests_dir.joinpath(f"{get_proper_file_name(job_name)}_tests_staging.csv")

        df_tests.to_csv(path_or_buf = output_file_tests, index=False, header=True, mode='w')

    if df_builds is not None:
        builds_dir = output_dir.joinpath('builds')
        builds_dir.mkdir(parents=True, exist_ok=True)
        output_file_builds = builds_dir.joinpath(f"{get_proper_file_name(job_name)}_builds_staging.csv")

        df_builds.to_csv(path_or_buf = output_file_builds, index=False, header = True, mode='w')

def get_jobs_info(name, server, is_job, output_dir_str):

    jobs_info = []

    if is_job:
        try:
            jobs = [server.get_job_info(name, depth= 0, fetch_all_builds = False)]
        except:
            print("ERROR: exception thrown when querying job from server")
            jobs = []

    # Not a job but a project's name
    else:
        regex = re.compile(f"^.*{name}.*$", re.IGNORECASE)
        jobs = server.get_job_info_regex(regex, folder_depth=0, depth=0)
    
    for job_info in jobs:

        # For -p argument instead of -a
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
            num_builds = 0 if 'builds' not in job_info else len(job_info['builds'])          # 0 <= num_builds <= 100
            if num_builds == 0:
                continue

            diff = None
            # Get latest build on file
            df = None
            p = Path(output_dir_str).joinpath("builds").joinpath(f"{get_proper_file_name(job_name)}_builds.csv")
            if p.exists():
                df = pd.read_csv(p.resolve(), dtype=JENKINS_BUILD_DTYPE, parse_dates=['commit_ts'], header=0)

                latest_build_on_file = df['build_number'].max()
                lastest_build_on_server = job_info['lastBuild']['number']
                diff = lastest_build_on_server - latest_build_on_file

                if diff <= 0:
                    continue
                elif diff >= 100:
                    full_job_info = server.get_job_info(job_name, depth= 0, fetch_all_builds = True)
                    jobs_info.append((full_job_info, latest_build_on_file))
                else:
                    jobs_info.append((job_info, latest_build_on_file))

            else:
                if num_builds == 100:
                    full_job_info = server.get_job_info(job_name, depth= 0, fetch_all_builds = True)
                    jobs_info.append((full_job_info, None))
                else:
                    jobs_info.append((job_info, None))

    return jobs_info

def get_all_job_names(server):
    jobs = server.get_all_jobs(folder_depth=2)
    job_names = []
    for job in jobs:
        class_ = job['_class'].split('.')[-1]
        if class_ in ['Folder', 'OrganizationFolder', 'WorkflowMultiBranchProject']:
            continue
        else:
            name = job['fullname']
            job_names.append(name)
    return job_names

def process_jobs(name, is_job, server, server_url, first_load, output_dir_str ='./data', build_only = False):

    for job_info, latest_build_on_file in get_jobs_info(name, server, is_job, output_dir_str= output_dir_str):

        latest_build_on_file = -1 if latest_build_on_file is None else latest_build_on_file
        fullName = job_info['fullName']
        print(f"\tJob: {fullName}")

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

        builds_data, tests_data = get_data(builds, fullName, server, server_url, build_only)
        print(f"\t\t{len(builds_data)} new builds.")

        df_builds = None
        if builds_data != []:
            df_builds = pd.DataFrame(data = builds_data, columns=list(JENKINS_BUILD_DTYPE.keys()))
            # Explicitly cast to Int64 since if there are None in columns of int type, they will be implicitly casted to float64
            df_builds = df_builds.astype({    
                "build_number" : "Int64",
                "duration" : "Int64",
                "estimated_duration" : "Int64",
                "test_pass_count" : "Int64",
                "test_fail_count" : "Int64",
                "test_skip_count" : "Int64"})
    
        df_tests = None    
        if tests_data != []:
            df_tests = pd.DataFrame(data = tests_data, columns=list(JENKINS_TEST_DTYPE.keys()))
            df_tests = df_tests.astype({"build_number" : "Int64"})
       
        write_to_file((fullName,df_builds, df_tests), output_dir_str, build_only)

def fetch_jenkins_data(all, server_url, projects_path, output_path, build_only):

    start = time.time()
    server = jenkins.Jenkins(server_url)

    # Sometimes connecting to Jenkins server is banned due to ill use of API
    # Test connection to server
    print(f"Jenkins-API version: {server.get_version()}")
    print(f"Fetching data from server: {server_url}")

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
        process_jobs(name, all, server, server_url, True, output_dir_str = output_path, build_only= build_only)

    end = time.time()
    print(f"Time total: {end-start}")

if __name__ == "__main__":

    ap = argparse.ArgumentParser(description="Scrip to fetch data from Apache Jenkins Server at https://builds.apache.org/")

    ap.add_argument("-o","--output-path", default='./data' , help="Path to output file directory, default to './data'")
    ap.add_argument("-b","--build-only",  help = "Write only build data.", action='store_true')
    ap.add_argument("-p","--projects", help = "Path to a file containing names of all projects to load, if not provided, load data from all jobs available on the server.")
    ap.add_argument("-s","--server", default="https://builds.apache.org/", help="URL to Jenkins server, default to https://builds.apache.org/")

    args = vars(ap.parse_args())

    build_only = args['build_only']
    output_path = args['output_path']
    projects_path = None if 'projects' not in args else args['projects']
    server_url = args['server']

    all = True if projects_path is None else False
    fetch_jenkins_data(all, server_url, projects_path, output_path, build_only)

