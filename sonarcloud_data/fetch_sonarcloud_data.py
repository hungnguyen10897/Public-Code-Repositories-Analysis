import requests
import math
from datetime import datetime, timedelta
from pathlib import Path
import sys
import pandas as pd
from collections import OrderedDict
import argparse
import numpy as np

SERVER = "https://sonarcloud.io/"
ORGANIZATION = "apache"
SONAR_MEASURE_DTYPE = OrderedDict({
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
    'test_execution_time': 'Int64',
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
    'development_cost': 'float64',
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
    'new_lines': 'object'})

SONAR_ISSUES_DTYPE = OrderedDict({
    "project" : "object",
    "analysis_key" : "object",
    "issue_key" : "object", 
    "type" : "object", 
    "rule" : "object", 
    "severity" : "object", 
    "status" : "object", 
    "resolution" : "object", 
    "effort" : "Int64", 
    "debt" : "Int64", 
    "tags" : "object", 
    "creation_date" : "object", 
    "update_date" : "object", 
    "close_date" :  "object"
})

SONAR_ANALYSES_DTYPE = OrderedDict({
    "project" : "object", 
    "analysis_key" : "object", 
    "date" : "object", 
    "project_version" : "object", 
    "revision" : "object"
})

def write_metrics_file(metric_list):
    metric_list.sort(key = lambda x: ('None' if 'domain' not in x else x['domain'], int(x['id'])))

    with open('./all_metrics.txt', 'w') as f:
        for metric in metric_list:
            # Ignore this, extremely long
            if metric == 'sonarjava_feedback':
                continue
            f.write("{} - {} - {} - {} - {}\n".format(
                'No ID' if 'id' not in metric else metric['id'],
                'No Domain' if 'domain' not in metric else metric['domain'],
                'No Key' if 'key' not in metric else metric['key'],
                'No Type' if 'type' not in metric else metric['type'],
                'No Description' if 'description' not in metric else metric['description']
                ))

def query_server(type, iter = 1, project_key = None, metric_list = [], from_ts = None):

    page_size = 200
    params = {'p' : iter, 'ps':page_size}
    if type == 'projects':
        endpoint = SERVER + "api/components/search"
        params['organization'] = ORGANIZATION
        params['qualifiers'] = 'TRK'

    elif type == 'metrics':
        endpoint = SERVER + "api/metrics/search"

    elif type == 'analyses':
        endpoint = SERVER + "api/project_analyses/search"
        if from_ts:
            params['from'] = from_ts
        params['project'] = project_key

    elif type == 'measures':
        endpoint = SERVER + "api/measures/search_history"
        if from_ts:
            params['from'] = from_ts
        params['component'] = project_key
        params['metrics'] = ','.join(metric_list)

    elif type == 'issues':
        endpoint = SERVER + "api/issues/search"
        params['componentKeys'] = project_key

    else:
        print("ERROR: Illegal info type.")
        return []

    r = requests.get(endpoint, params=params)

    if r.status_code != 200:
        print(f"ERROR: HTTP Response code {r.status_code} for request {r.request.path_url}")
        return []

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
    elif type == 'measures':
        element_list = r_dict['measures']
        total_num_elements = r_dict['paging']['total']
    elif type == 'issues':
        element_list = r_dict['issues']
        total_num_elements = r_dict['paging']['total']

    if iter*page_size < total_num_elements:
        if type == 'measures':
            element_list = concat_measures(element_list, query_server(type, iter+1, project_key, from_ts = from_ts))
        else:
            element_list = element_list + query_server(type, iter+1, project_key, from_ts = from_ts)
    
    return element_list

def concat_measures(measures_1,measures_2):
    for measure_1, measure_2 in zip(measures_1, measures_2):
        measure_1['history'] = measure_1['history'] + measure_2['history']
    return measures_1

def process_datetime(time_str):
    if time_str is None:
        return None

    ts = datetime.strptime(time_str[:19], "%Y-%m-%dT%H:%M:%S")

    offset = timedelta(hours = int(time_str[20:22]), minutes = int(time_str[22:24]))

    if time_str[19] == '-':
        ts = ts + offset
    elif time_str[19] == '+':
        ts = ts - offset
    
    return ts

def load_metrics(path = None):
    if path is None:
        path = './metrics.txt'
    p = Path(path)
    if not p.exists():
        print(f"ERROR: Path for metrics {p.resolve()} does not exists.")
        sys.exit(1)
    try:
        metrics_order = {}
        with open(p, 'r') as f:
            order = 0
            for line in f:
                parts = line.split(' - ')
                metric = parts[2]
                type = parts[3]
                metrics_order[metric] = (order,type)
                order += 1
        return metrics_order
    except:
        print("ERROR: Reading metrics file")
        sys.exit(1)

def get_duration_from_str(input_str):

    if input_str is not None:
        idx_min = input_str.find('min')
        idx_h = input_str.find('h')
        idx_d = input_str.find('d')

        if idx_d != -1:
            days = int(input_str[:idx_d])
            if len(input_str) == idx_d + 1:
                return 24*60*days
            return 24*60*days + get_duration_from_str(input_str[idx_d + 1:])

        if idx_h != -1:
            hours = int(input_str[:idx_h])
            if len(input_str) == idx_h + 1:
                return 60*hours           
            return 60*hours + get_duration_from_str(input_str[idx_h + 1:])

        if idx_min != -1:
            mins = int(input_str[:idx_min])
            return mins

        print(f"ERROR: duration string '{input_str}' does not contain 'min', 'h' or 'd'.")
        sys.exit(1)

def safe_cast(val, to_type, contain_comma = False):
    if to_type in ['INT' ,'WORK_DUR']:
        try:
            return int(val)
        except (ValueError, TypeError):
            print(f"WARNING: exception casting value {str(val)} to type {to_type}")
            return None
    elif to_type in ['FLOAT', 'PERCENT', 'RATING']:
        try:
            return float(val)
        except (ValueError, TypeError):
            print(f"WARNING: exception casting value {str(val)} to type {to_type}")
            return None
    elif to_type == 'BOOL':
        try:
            return bool(val)
        except (ValueError, TypeError):
            print(f"WARNING: exception casting value {str(val)} to type {to_type}")
            return None
    elif to_type == 'MILLISEC':
        try:
            if len(val) >= 12:
                return datetime.fromtimestamp(int(val)/1000)
            else:
                return int(val)
        except (ValueError, TypeError):
            print(f"WARNING: exception casting value {str(val)} to type {to_type}")
            return None
    else:
        try:
            return str(val) if not contain_comma else str(val).replace(',',';')
        except (ValueError, TypeError):
            print(f"ERROR: error casting to type {to_type}")
            return None

def extract_measures_value(measures, metrics_order_type, columns, data):

    for measure in measures:

        metric = measure['metric']
        type = metrics_order_type[metric][1]
    
        columns.append(metric)
        history = measure['history']

        contain_comma = False
        if metric in ['quality_profiles','quality_gate_details']:
            contain_comma = True

        values = list((map(lambda x: None if 'value' not in x else safe_cast(x['value'],type, contain_comma), history)))
        values.reverse()
        values = values[:len(data['analysis_key'])]

        # Resolving None Integer values
        if SONAR_MEASURE_DTYPE[metric] == "Int64":
            data[metric] = pd.array(values, dtype=pd.Int64Dtype())
        else:
            data[metric] = values
    
    return columns, data

def get_analysis_key(update_date, key_date_list):

    update_date = np.datetime64(update_date)

    for i in range(len(key_date_list)):

        analysis_date = key_date_list[i][1]
        
        if update_date > analysis_date:
            return key_date_list[i-1][0]
            
    return key_date_list[-1][0]

def process_project_measures(project, output_path, new_analyses, metrics_path = None ):

    project_key = project['key']

    output_path = Path(output_path).joinpath("measures")
    output_path.mkdir(parents=True, exist_ok=True)
    staging_file_path = output_path.joinpath(f"{project_key.replace(' ','_').replace(':','_')}_staging.csv")

    min_ts_str = new_analyses['date'].min().strftime(format = '%Y-%m-%d')

    metrics_order_type = load_metrics(metrics_path)
    metrics = list(metrics_order_type.keys())

    measures = []
    for i in range(0,len(metrics), 15):
        #Get measures
        measures = measures + query_server('measures',1,project_key, metrics[i:i+15], from_ts= min_ts_str)
    
    measures.sort(key = lambda x: metrics_order_type[x['metric']][0])

    data = OrderedDict()
    data['project'] = [project_key] * len(new_analyses)
    data['analysis_key'] = new_analyses['analysis_key'].values.tolist()

    columns = ['project', 'analysis_key']

    columns_with_metrics, data_with_measures = extract_measures_value(measures, metrics_order_type, columns, data)

    #Create DF
    df = pd.DataFrame(data_with_measures, columns= columns_with_metrics)
    df.to_csv(path_or_buf= staging_file_path, index=False, header=True)

def process_project_issues(project, output_path, new_analyses, latest_analysis_ts_on_file):

    project_key = project['key']

    output_path = Path(output_path).joinpath("issues")
    output_path.mkdir(parents=True, exist_ok=True)
    file_path = output_path.joinpath(f"{project_key.replace(' ','_').replace(':','_')}_staging.csv")

    project_issues = query_server('issues', 1, project_key = project_key)

    new_analysis_keys = new_analyses['analysis_key'].values.tolist()
    new_analysis_dates = new_analyses['date'].values
    # dates are in decreasing order
    key_date_list = list(zip(new_analysis_keys, new_analysis_dates))

    issues = []
    for project_issue in project_issues:

        creation_date = None if 'creationDate' not in project_issue else process_datetime(project_issue['creationDate'])
        update_date = None if 'updateDate' not in project_issue else process_datetime(project_issue['updateDate'])
        
        # belong to the analyses on file
        if update_date is not None and latest_analysis_ts_on_file is not None and update_date <= latest_analysis_ts_on_file:
            continue

        analysis_key = None if update_date is None else get_analysis_key(update_date, key_date_list)

        close_date = None if 'closeDate' not in project_issue else process_datetime(project_issue['closeDate'])

        issue_key = None if 'key' not in project_issue else project_issue['key']
        rule = None if 'rule' not in project_issue else project_issue['rule']
        severity = None if 'severity' not in project_issue else project_issue['severity']
        status = None if 'status' not in project_issue else project_issue['status']
        resolution = None if 'resolution' not in project_issue else project_issue['resolution']

        effort = None if 'effort' not in project_issue else get_duration_from_str(project_issue['effort'])
        debt = None if 'debt' not in project_issue else get_duration_from_str(project_issue['debt'])

        if 'tags' not in project_issue or len(project_issue['tags']) == 0:
            tags = None
        else:
            tags = ','.join(project_issue['tags'])

        type = None if 'type' not in project_issue else project_issue['type']
     
        issue = (project_key, analysis_key, issue_key, type, rule, severity, status, resolution, effort, debt, tags, creation_date, update_date, close_date)
        issues.append(issue)

    print(f"\t\t{project_key} - {len(issues)} new issues")
    if issues != []:
        df = pd.DataFrame(data = issues, columns= SONAR_ISSUES_DTYPE.keys())
        df = df.astype({
            "effort" : "Int64",
            "debt" : "Int64"
        })

        df.to_csv(file_path, index=False, header=True)

def process_project_analyses(project, output_path):

    project_key = project['key']

    output_path = Path(output_path).joinpath("analyses")
    output_path.mkdir(parents=True, exist_ok=True)
    staging_file_path = output_path.joinpath(f"{project_key.replace(' ','_').replace(':','_')}_staging.csv")
    archive_file_path = output_path.joinpath(f"{project_key.replace(' ','_').replace(':','_')}.csv")

    last_analysis_ts = None
    if archive_file_path.exists():
        try:
            old_df = pd.read_csv(archive_file_path.resolve(), dtype=SONAR_ANALYSES_DTYPE, parse_dates=['date'])
            last_analysis_ts = old_df['date'].max()

        except ValueError as e:
            print(f"\t\tERROR: {e} when parsing {archive_file_path} into DataFrame.")

        except FileNotFoundError as e:
            # print(f"\t\tWARNING: No .{format} file found for project {project_key} in output path for")
            pass

    lines = []
    from_ts = None if last_analysis_ts is None else last_analysis_ts.strftime(format = '%Y-%m-%d')
    analyses = query_server('analyses',1, project_key = project_key, from_ts = from_ts)
    for analysis in analyses:
        analysis_key = None if 'key' not in analysis else analysis['key']
        
        date = None if 'date' not in analysis else process_datetime(analysis['date'])
        if date is not None and last_analysis_ts is not None:
            if date <= last_analysis_ts:
                continue

        project_version = None if 'projectVersion' not in analysis else analysis['projectVersion']
        revision = None if 'revision' not in analysis else analysis['revision']

        line = (project_key, analysis_key, date, project_version, revision)
        lines.append(line)
    
    print(f"\t\t {project_key} - {len(lines)} new analyses.")
    if lines != []:
        df = pd.DataFrame(data = lines, columns= SONAR_ANALYSES_DTYPE.keys())
        df.to_csv(staging_file_path, index= False, header=True)
        return df, last_analysis_ts
    
    return None, last_analysis_ts

def fetch_sonar_data(output_path):

    project_list = query_server(type='projects')
    project_list.sort(key = lambda x: x['key'])

    print(f"Total: {len(project_list)} projects.")
    i = 0
    for project in project_list:
        print(f"\t{i}: ")
        new_analyses, latest_analysis_ts_on_file = process_project_analyses(project, output_path)
        if new_analyses is None:
            continue
        process_project_measures(project, output_path, new_analyses)
        process_project_issues(project, output_path, new_analyses, latest_analysis_ts_on_file)
        i += 1

if __name__ == "__main__":

    ap = argparse.ArgumentParser()
    ap.add_argument("-o","--output-path", default='./data' , help="Path to output file directory.")

    args = vars(ap.parse_args())

    output_path = args['output_path']

    # Write all metrics to a file
    # write_metrics_file(query_server(type='metrics'))

    fetch_sonar_data(output_path)

