import sys, os, argparse, psycopg2
if "PRA_HOME" not in os.environ:
    print("Please set environment variable PRA_HOME before running.")
    sys.exit(1)

project_path = os.environ['PRA_HOME']
sys.path.append(project_path)

# Sonar Miner path
sys.path.append("")

from jenkins_data.fetch_jenkins_data import fetch_jenkins_data
from sonar_src import fetch_sonar_data

from pathlib import Path
from orchestration_utils import CONNECTION_OBJECT

def run(source, connection_object = CONNECTION_OBJECT, data_dir = f"{project_path}/data"):
    try:
        conn = psycopg2.connect(
            host=connection_object["host"],
            database=connection_object["database"],
            user=connection_object["user"],
            password=connection_object["password"])
        cursor = conn.cursor()

        if source == "sonarcloud":
            cursor.execute("SELECT DISTINCT sonar_org_key FROM source")
            sonar_org_keys = cursor.fetchall()

            for row in sonar_org_keys:
                key = row[0]
                path = Path(data_dir).joinpath("sonarcloud").joinpath(key).absolute()
                fetch_sonar_data(path, organization=key)

        elif source == "jenkins":
            cursor.execute("SELECT DISTINCT jenkins_server FROM source")
            jenkins_servers = cursor.fetchall()

            for row in jenkins_servers:
                server = row[0]
                server_folder_name = server.split('/')[2]
                path = Path(data_dir).joinpath("jenkins").joinpath(server_folder_name).absolute()
                fetch_jenkins_data(all=True, server_url = server, projects_path=None, output_path= path, build_only=True) 
        
        else:
            print("Inappropriate source")
            sys.exit(1)

    except (Exception, psycopg2.Error) as error:
        print(f"Error while connectiong to PRA database: {error}")
    finally:
        if(conn):
            cursor.close()
            conn.close()
    

if __name__ == "__main__":

    ap = argparse.ArgumentParser(description="Script to fetch new data from Jenkins and Sonarcli.")
    ap.add_argument("-d","--data-directory", default=f'{project_path}/data' , help="Path to data directory.")

    args = vars(ap.parse_args())
    data_dir = args['data_directory']
    run("sonarcloud",data_dir=data_dir)
    run("jenkins",data_dir=data_dir)
