import sys
import jenkins
import re
import configparser
import csv
from pathlib import Path

def get_projects(path):
    projects = []
    with open(path, 'r') as f:
        for line in f:
            project_name = line.strip()
            projects.append(project_name)

    return projects

def extract_test_data(test_report, job_name, build_number):

    if not test_report:
        return (None,None)

    test_duration = test_report['duration']

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

def get_data(builds , server):
    
    builds_tests_data = []

    for build in builds:

        job_name = build['fullDisplayName'].split('#')[0].strip()
        build_number = int(build['id'])
        build_result = build['result']
        build_duration = build['duration']
        build_estimated_duration = build['estimatedDuration']

        # Get revision number
        revision_number = None

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

        changeset_items = build['changeSet']['items']
        for item in changeset_items:
            if 'commitId' in item:
                if 'date' in item:
                    commit_ids_ts[item['commitId']] = item['date']
                else:
                    commit_ids_ts[item['commitId']] = None

        if len(commit_ids_ts) != 1:
            print(f"WARNING: {len(commit_ids_ts)} commits ids found for the build \
                {build['fullDisplayName']}")

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
                    duration, child_test_result = extract_test_data(child_report['result'], job_name, build_number)
                    if duration is not None:
                        if build_total_test_duration is None:
                            build_total_test_duration = duration
                        else:
                            build_total_test_duration += duration
                    if child_test_result is not None:
                        test_result = test_result + child_test_result

            elif test_report_class == "TestResult":
                build_total_test_duration, test_result = extract_test_data(test_report, job_name, build_number)

            else:
                print(f"WARNING: Unrecognized test report class for {job_name} - {str(build_number)}")
        
        # There can be multiple lines for a build of a job due to multiple commits
        if len(commit_ids_ts) > 0:
            for commit_id, ts in commit_ids_ts:
                build_data = (job_name, build_number, build_result, build_duration, build_estimated_duration, \
                    revision_number, commit_id, ts, build_pass_count, build_fail_count, build_skip_count, build_total_test_duration)
        
        else:
            build_data = (job_name, build_number, build_result, build_duration, build_estimated_duration, \
                    revision_number, None, None, build_pass_count, build_fail_count, build_skip_count, build_total_test_duration)
        
        builds_tests_data.append((build_data, test_result))

    return builds_tests_data

def write_to_csv(project_data, output_dir_str='./data/'):
    
    project, jobs_data = project_data

    output_dir = Path(output_dir_str)
    output_file_builds = output_dir.joinpath(f'builds/{project}_builds.csv')
    output_file_tests = output_dir.joinpath(f'tests/{project}_tests.csv')

    build_file_fields = ["job", "build_number", "result", "duration", "estimated_duration", \
            "revision_number", "commit_id", "test_pass_count", "test_fail_count", "test_skip_count", \
                "total_test_duration"]

    test_file_fields = ["job", "build_number","package", "class","name", "duration", "status"]

    # Write both builds and tests data
    with open(output_file_builds, 'w', newline="") as build_file:
        with open(output_file_builds, 'w', newline="") as test_file:

            writer_build = csv.writer(build_file)
            writer_test = csv.writer(test_file)

            writer_build.writerow(build_file_fields)
            writer_test.writerow(test_file_fields)

            for build_data, test_data in jobs_data:
                writer_build.writerow(build_data)
                
                for test_result in test_data:
                    writer_test.writerow(test_result)


def get_jobs_info(project, server, first_load, sub_project=False):

    if sub_project:
        return [server.get_job_info(project, depth= 2, fetch_all_builds = first_load)]

    # Not a sub_project
    jobs_info = []
    regex = re.compile(f"^.*{project}.*$", re.IGNORECASE)

    depth = 0 if first_load else 2
    jobs_info_regex = server.get_job_info_regex(regex, folder_depth=0, depth=depth)
        
    for job_info in jobs_info_regex:

        class_ = job_info['_class'].split('.')[-1]
        if class_ in ['Folder', 'OrganizationFolder', 'WorkflowMultiBranchProject']:
            for sub_job in job_info['jobs']:
                fullName = sub_job['fullName']
                jobs_info += get_jobs_info(fullName, server, first_load, True)

        else:
            if first_load:
                jobs_info.append(server.get_job_info(job_info['fullName'], depth= 2, fetch_all_builds = True))
            else:
                jobs_info.append(job_info)

    return jobs_info

def process_project(project, server, first_load=False):

    jobs_data = []

    for job_info in get_jobs_info(project, server, first_load):

        fullName = job_info['fullName']

        # 'scm' field checking
        if 'scm' in job_info and '_class' in job_info['scm']:
            scm_class = job_info['scm']['_class'].split('.')[-1]
            if scm_class in ['SubversionSCM','NullSCM']:
                continue

        # job_data contains both build and test data
        job_data = get_data(job_info['builds'], server)
    
        jobs_data.append = job_data
    
    write_to_csv((project,jobs_data))

if __name__ == "__main__":

    server = jenkins.Jenkins('https://builds.apache.org/')

    # Sometimes connecting to Jenkins server is banned due to ill use of API
    # Test connection to server
    print(f"Jenkins-API version: {server.get_version()}")

    if len(sys.argv)  != 2:
        print("Provide also path to one csv file with projects' names in it.") 
        sys.exit(1)

    projects = get_projects(sys.argv[1])    
    print(projects)

    for project in projects:
        process_project(project, server, first_load = True)