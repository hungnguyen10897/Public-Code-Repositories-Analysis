"""
This script marks column [processed] of all rows of the following tables as 'TRUE': jenkins_builds, sonar_analyses,
sonar_issues, sonar_measures
"""

import psycopg2
from utils import get_connection_object

def run():
    try:
        conn = psycopg2.connect(
            host="localhost",
            database="pra2",
            user="pra",
            password="pra")

        cursor = conn.cursor()

        cursor.execute("UPDATE jenkins_builds SET processed = TRUE")
        cursor.execute("UPDATE sonar_analyses SET processed = TRUE")
        cursor.execute("UPDATE sonar_issues SET processed = TRUE")
        cursor.execute("UPDATE sonar_measures SET processed = TRUE")

    except (Exception, psycopg2.Error) as error:
        print(f"Error while connectiong to PRA database: {error}")
    finally:
        if(conn):
            cursor.close()
            conn.close()

if __name__ == "__main__":
    run()