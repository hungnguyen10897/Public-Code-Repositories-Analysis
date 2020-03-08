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

def process_project(project, server):
    regex = re.compile(f"^.*{project}.*$", re.IGNORECASE)

    jobs_info = server.get_job_info_regex(regex, folder_depth=0, depth=0)

    for job_info in jobs_info:
        class_ = job_info['_class'].split('.')[-1]
        name = job_info['fullName']

        if class_ in ['Folder', 'Folder', 'OrganizationFolder', 'WorkflowMultiBranchProject']:
            #There should be a "jobs" field containing more jobs:
            pass

        else:
            builds_data = job_info['builds']


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

    # for project in projects:
    #     process_project(project, server)