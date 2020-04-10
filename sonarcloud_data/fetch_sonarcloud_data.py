import requests
import math
from datetime import datetime, timedelta
from pathlib import Path
import sys
import pandas as pd
from collections import OrderedDict
import argparse

SERVER = "https://sonarcloud.io/"
ORGANIZATION = "apache"
SONAR_DTYPE = {'project': 'object',
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
    'test_execution_time': 'object',
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
    'development_cost': 'object',
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
    'new_lines': 'object'}


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
    elif type == 'measures':
        element_list = r_dict['measures']
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
        path = './all_metrics.txt'
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
    elif type == 'MILLISEC':
        try:
            return datetime.fromtimestamp(int(val)/1000)
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
        contain_comma = False
        if metric in ['quality_profiles','quality_gate_details']:
            contain_comma = True
        type = metrics_order_type[metric][1]

        columns.append(metric)
        history = measure['history']
        values = list((map(lambda x: None if 'value' not in x else safe_cast(x['value'],type, contain_comma), history)))
        values.reverse()
        data[metric] = values
    
    return columns, data

def process_project(project, format, output_path, metrics_path = None ):

    project_key = project['key']

    output_path_format = Path(output_path).joinpath(format)
    output_path_format.mkdir(parents=True, exist_ok=True)
    staging_file_path = output_path_format.joinpath(f"{project_key}_staging.{format}")
    archive_file_path = output_path_format.joinpath(f"{project_key}.{format}")

    max_ts_str = None

    if archive_file_path.exists():
        try:
            if format == 'csv':
                old_df = pd.read_csv(archive_file_path.resolve(), dtype=SONAR_DTYPE, parse_dates=['date'])
            elif format == 'parquet':
                old_df = pd.read_parquet(path = archive_file_path.resolve())
            # TO_DO: Change nan to None ? Is it neccessary?
            max_ts_str = old_df['date'].max().strftime(format = '%Y-%m-%d')
        except ValueError as e:
            print(f"\t\tERROR: {e} when parsing {archive_file_path} into DataFrame.")
            max_ts_str = None

        except FileNotFoundError as e:
            # print(f"\t\tWARNING: No .{format} file found for project {project_key} in output path for")
            max_ts_str = None

    project_analyses = query_server('analyses', 1, project_key = project_key, from_ts = max_ts_str)
    print(f"\t\t{project_key} - {len(project_analyses)} analyses")

    if len(project_analyses) == 0:
        return

    revision_list = []
    date_list = []
    version_list = []
    for analysis in project_analyses:
        revision = None if 'revision' not in analysis else analysis['revision']
        revision_list.append(revision)
        date = None if 'date' not in analysis else process_datetime(analysis['date'])
        date_list.append(date)
        version = None if 'projectVersion' not in analysis else analysis['projectVersion']
        version_list.append(version)
    
    metrics_order_type = load_metrics(metrics_path)
    metrics = list(metrics_order_type.keys())

    measures = []
    for i in range(0,len(metrics), 15):
        #Get measures
        measures = measures + query_server('measures',1,project_key, metrics[i:i+15], from_ts= max_ts_str)
    
    measures.sort(key = lambda x: metrics_order_type[x['metric']][0])

    data = OrderedDict()
    data['project'] = [project_key] * len(project_analyses)
    data['version'] = version_list
    data['date'] = date_list
    data['revision'] = revision_list
    columns = ['project', 'version', 'date', 'revision']

    columns_with_metrics, data_with_measures = extract_measures_value(measures, metrics_order_type, columns, data)

    #Create DF
    df = pd.DataFrame(data_with_measures, columns= columns_with_metrics)

    if format == "csv":
        df.to_csv(path_or_buf= staging_file_path, index=False, header=True)
    elif format == "parquet":
        df.to_parquet(fname= staging_file_path, index=False)

def write_metrics_file(metric_list):
    metric_list.sort(key = lambda x: ('None' if 'domain' not in x else x['domain'], int(x['id'])))

    with open('./all_metrics.txt', 'w') as f:
        for metric in metric_list:
            f.write("{} - {} - {} - {} - {}\n".format(
                'No ID' if 'id' not in metric else metric['id'],
                'No Domain' if 'domain' not in metric else metric['domain'],
                'No Key' if 'key' not in metric else metric['key'],
                'No Type' if 'type' not in metric else metric['type'],
                'No Description' if 'description' not in metric else metric['description']
                ))

def fetch_sonarqube(format, output_path):
    project_list = query_server(type='projects')
    project_list.sort(key = lambda x: x['key'])

    print(f"Total: {len(project_list)} projects.")
    i = 0
    for project in project_list:
        print(f"\t{i}: ")
        process_project(project, format, output_path)
        i += 1

if __name__ == "__main__":

    ap = argparse.ArgumentParser()

    ap.add_argument("-f","--format", choices=['csv', 'parquet'], default='csv', 
        help="Output file format. Can either be csv or parquet")

    ap.add_argument("-o","--output-path", default='./data' , help="Path to output file directory.")
    # ap.add_argument("-l","--load", choices = ['first', 'incremental'], default='incremental' , help="Path to output file directory.")

    args = vars(ap.parse_args())
    format = args['format']
    output_path = args['output_path']
    # load = args['load']

    # Write all metrics to a file
    # write_metrics_file(query_server(type='metrics'))

    fetch_sonarqube(format, output_path)

