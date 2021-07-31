import os, sys, argparse, psycopg2
from pathlib import Path

assert "PRA_HOME" in os.environ
assert os.environ["PRA_HOME"] in sys.path

from utils import PRA_HOME, CONNECTION_OBJECT
from extractors.jenkins.fetch_jenkins_data import fetch_jenkins_data
from extractors.sonarqube.sonar_src import fetch_organization_sonar_data

def run(source, connection_object = CONNECTION_OBJECT, data_dir = f"{PRA_HOME}/data"):
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
                fetch_organization_sonar_data(output_path= path, organization=key)

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
        raise error
    finally:
        if(conn):
            cursor.close()
            conn.close()
    
if __name__ == "__main__":

    ap = argparse.ArgumentParser(description="Script to fetch new data from Jenkins and Sonarcloud.")
    ap.add_argument("-d","--data-directory", default=f'{PRA_HOME}/data' , help="Path to data directory.")

    args = vars(ap.parse_args())
    data_dir = args['data_directory']
    run("sonarcloud",data_dir=data_dir)
    run("jenkins",data_dir=data_dir)
