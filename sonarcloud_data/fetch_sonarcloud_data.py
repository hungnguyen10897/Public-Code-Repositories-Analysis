import requests
import math

SERVER = "https://sonarcloud.io/"

def get_all_projects():
    page_size = 200
    params = {'organization':'apache', 'qualifiers': 'TRK', 'p':1, 'ps': page_size}

    r = requests.get(SERVER + "api/components/search", params=params)

    if r.status_code != 200:
        print(f"ERROR: HTTP Response code {r.status_code} for request {r.request.path_url}")
    else:
        r_dict = r.json()
    
    project_list = r_dict['components']
    
    total_num_projects = r_dict['paging']['total']
    iter_num = math.ceil(total_num_projects/page_size)

    for i in range(2,iter_num + 1):
        params['p'] = i
        r = requests.get(SERVER + "api/components/search", params=params)
        if r.status_code != 200:
            print(f"ERROR: HTTP Response code {r.status_code} for request {r.request.path_url}")
        else:
            r_dict = r.json()
            project_list = project_list + r_dict['components']
    
    return project_list

if __name__ == "__main__":

    project_list = get_all_projects()
    
    for project in project_list:
        pass
