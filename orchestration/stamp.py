"""
This script marks column [processed] of all rows of the following tables as 'TRUE': jenkins_builds, sonar_analyses,
sonar_issues, sonar_measures
"""

import psycopg2
from orchestration_utils import CONNECTION_OBJECT

def run(connection_object = CONNECTION_OBJECT):
    try:
        conn = psycopg2.connect(
            host=connection_object["host"],
            database=connection_object["database"],
            user=connection_object["user"],
            password=connection_object["password"])
        cursor = conn.cursor()

        print("Stamping jenkins_builds")
        cursor.execute("UPDATE jenkins_builds SET processed = TRUE")
        print("Stamping sonar_analyses")
        cursor.execute("UPDATE sonar_analyses SET processed = TRUE")
        print("Stamping sonar_issues")
        cursor.execute("UPDATE sonar_issues SET processed = TRUE")
        print("Stamping sonar_measures")
        cursor.execute("UPDATE sonar_measures SET processed = TRUE")

    except (Exception, psycopg2.Error) as error:
        print(f"Error while connectiong to PRA database: {error}")
    finally:
        if(conn):
            cursor.close()
            conn.close()

if __name__ == "__main__":
    run()