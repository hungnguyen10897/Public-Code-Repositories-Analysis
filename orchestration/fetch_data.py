import sys, os
if "PRA_HOME" not in os.environ:
    print("Please set environment variable PRA_HOME before running.")
    sys.exit(1)

project_path = os.environ['PRA_HOME']
sys.path.append(project_path)

from jenkins_data.fetch_jenkins_data import fetch_jenkins_data
from sonarcloud_data.fetch_sonarcloud_data import fetch_sonar_data

import psycopg2
from pathlib import Path

if __name__ == "__main__":

    try:
        conn = psycopg2.connect(
            host="localhost",
            database="pra2",
            user="pra",
            password="pra")

        cursor = conn.cursor()

        cursor.execute("SELECT DISTINCT sonar_org_key FROM source")
        sonar_org_keys = cursor.fetchall()

        for row in sonar_org_keys:
            key = row[0]
            path = Path(project_path).joinpath("data").joinpath("sonarcloud").joinpath(key).absolute()
            fetch_sonar_data(path, organization=key)

        cursor.execute("SELECT DISTINCT jenkins_server FROM source")
        jenkins_servers = cursor.fetchall()

        for row in jenkins_servers:
            server = row[0]
            server_folder_name = server.split('/')[2]
            path = Path(project_path).joinpath("data").joinpath("jenkins").joinpath(server_folder_name).absolute()
            fetch_jenkins_data(all=True, server_url = server, projects_path=None, output_path= path, build_only=True) 

    except (Exception, psycopg2.Error) as error:
        print(f"Error while connectiong to PRA database: {error}")
    finally:
        if(conn):
            cursor.close()
            conn.close()

