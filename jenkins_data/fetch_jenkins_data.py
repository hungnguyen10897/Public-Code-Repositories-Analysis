import sys
import jenkins
import re

def get_projects(path):
    projects = []
    with open(path, 'r') as f:
        for line in f:
            project_name = line.strip()
            projects.append(project_name)

    return projects

def get_build_data(builds):
    
    builds_data = []

    for build_data in builds:

        result = {}

        result['build_number'] = build_data['id']
        result['build_result'] = build_data['result']
        result['build_duration'] = build_data['duration']
        result['build_estimated_duration'] = build_data['estimatedDuration']

        fail_count = None
        skip_count = None
        total_count = None

        revision_number = None

        build_actions = build_data['actions']
    
        for action in build_actions:
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

        # Info about commits
        commit_ids_ts = {}

        changeset_items = build_data['changeSet']['items']
        for item in changeset_items:
            if 'commitId' in item:
                if 'date' in item:
                    commit_ids_ts[item['commitId']] = item['date']
                else:
                    commit_ids_ts[item['commitId']] = None

        if len(commit_ids_ts) != 1:
            print(f"WARNING: {len(commit_ids_ts)} commits ids found for the build \
                {build_data['fullDisplayName']}")

        result['test_fail_count'] = fail_count
        result['test_skip_count'] = skip_count
        result['test_total_count'] = total_count
        result['build_revision_number'] = revision_number
        result['build_commit_ids_ts'] = commit_ids_ts

        builds_data.append(result)

    return builds_data


def get_test_data():
    pass

def write_to_csv(project_info):
    pass

def process_project(project, server, sub_project=False):

    regex = re.compile(f"^.*{project}.*$", re.IGNORECASE)

    # depth = 2 to extract some more info to avoid querying the server many times
    jobs_info = server.get_job_info_regex(regex, folder_depth=0, depth=2)

    project_info = {}

    for job_info in jobs_info:

        class_ = job_info['_class'].split('.')[-1]
        fullName = job_info['fullName']

        if class_ in ['Folder', 'Folder', 'OrganizationFolder', 'WorkflowMultiBranchProject']:
            # There should be a "jobs" field containing more jobs:
            # drill again using server.get_job_info(name, depth=2)
            # rather than doing regex again

            # process_project(server, job_info['jobs'], True)
            pass

        else:
            # 'scm' field is only available for these types of classes:

            if 'scm' in job_info and '_class' in job_info['scm']:

                scm_class = job_info['scm']
                if scm_class.split('.')[-1] in ['SubversionSCM','NullSCM']:
                    continue

                # # Checking git source project
                # # Not taking into account MultiSCMs class
                # git_url = job_info['scm']['userRemoteConfigs'][0]['url']
                # git_project_name = git_url.split('/')[-1].split('.')[0].strip()
                # if git_project_name.lower() != project.lower():
                #     continue

            builds_data = get_build_data(job_info['builds'])
            test_data = get_test_data()
    
        project_info[fullName] = (builds_data, test_data)
    
    write_to_csv(project_info)

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
        process_project(project, server)