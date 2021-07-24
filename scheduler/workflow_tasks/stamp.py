"""
This script marks column [processed] of all rows of the following tables as 'TRUE': jenkins_builds, sonar_analyses,
sonar_issues, sonar_measures
"""

import os, sys, psycopg2

assert "PRA_HOME" in os.environ
assert os.environ["PRA_HOME"] in sys.path

from utils import CONNECTION_OBJECT

def run(connection_object = CONNECTION_OBJECT):
    try:
        conn = psycopg2.connect(
            host=connection_object["host"],
            database=connection_object["database"],
            user=connection_object["user"],
            password=connection_object["password"])
        cursor = conn.cursor()

        for table in ['jenkins_builds', 'sonar_analyses', 'sonar_issues', 'sonar_measures']:
            cursor.execute(f"UPDATE {table} SET processed = TRUE")
            conn.commit()
            print(f"Stamping {table} - {cursor.rowcount} records updated")

    except (Exception, psycopg2.Error) as error:
        print(f"Error while connectiong to PRA database: {error}")
    finally:
        if(conn):
            cursor.close()
            conn.close()

if __name__ == "__main__":
    run()