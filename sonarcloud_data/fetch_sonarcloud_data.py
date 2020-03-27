import requests
import math

SERVER = "https://sonarcloud.io/"
ORGANIZATION = "apache"

def get_server_info(type, iter = 1, project_key = None):

    page_size = 500
    params = {'p' : iter, 'ps':page_size}
    if type == 'projects':
        endpoint = SERVER + "api/components/search"
        params['organization'] = ORGANIZATION
        params['qualifiers'] = 'TRK'

    elif type == 'metrics':
        endpoint = SERVER + "api/metrics/search"

    elif type == 'analyses':
        endpoint = SERVER + "api/project_analyses/search"
        params['project'] = project_key

    else:
        print("ERROR: Illegal info type.")
        return None

    r = requests.get(endpoint, params=params)

    if r.status_code != 200:
        print(f"ERROR: HTTP Response code {r.status_code} for request {r.request.path_url}")
        return None

    r_dict = r.json()

    if type == 'projects':
        element_list = r_dict['components']
        total_num_elements = r_dict['paging']['total']
    elif type == 'metrics':
        element_list = r_dict['metrics']
        total_num_elements = r_dict['total']
    elif type == 'analyses':
        element_list = r_dict['analyses']
        total_num_elements = r_dict['paging']['total']

    if iter*page_size < total_num_elements:
        element_list = element_list + get_server_info(type, iter+1)
    
    return element_list

def process_project(project):

    project_key = project['key']
    project_analyses = get_server_info('analyses', 1, project_key = project_key)

    for analysis in project_analyses:
        pass

if __name__ == "__main__":

    # metric_list = get_server_info(type='metrics')
    # metric_list.sort(key = lambda x: ('None' if 'domain' not in x else x['domain'], int(x['id'])))

    # # Write metrics to a file
    # with open('./metrics.txt', 'w') as f:
    #     for metric in metric_list:
    #         f.write("{} - {} - {} - {}\n".format(
    #             'No ID' if 'id' not in metric else metric['id'],
    #             'No Domain' if 'domain' not in metric else metric['domain'],
    #             'No Key' if 'key' not in metric else metric['key'],
    #             'No Description' if 'description' not in metric else metric['description']
    #             ))

    project_list = get_server_info(type='projects')
    project_list.sort(key = lambda x: x['key'])

    for project in project_list:
        process_project(project)

