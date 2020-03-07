import sys
import jenkins

def get_jobs(path, server):
    jobs = []
    with open(path, 'r') as f:
        for line in f:
            project_name = line.strip()

            # If the project contains many jobs, the name is a view
            if server.view_exists(project_name) and not server.get_job_name(project_name):
                server.get_jobs(folder_depth=0, view_name= project_name)

            # The project itself is a job
            else:
                jobs.append(project_name)

    return jobs

def process_job(job, server):
    pass

if __name__ == "__main__":

    server = jenkins.Jenkins('https://builds.apache.org/')

    # Sometimes connecting to Jenkins server is banned due to ill use of API
    # Test connection to server
    print(f"Jenkins-API version: {server.get_version()}")

    if len(sys.argv != 2):
        print("Provide also path to one csv file with projects' names in it.") 
        sys.exit(1)

    jobs = get_jobs(sys.argv[1], server)    

    for job in jobs:
        process_job(job, server)