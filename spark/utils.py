import psycopg2
from spark_constants import JENKINS_BUILD_DTYPE, SONAR_ANALYSES_DTYPE, SONAR_ISSUES_DTYPE, SONAR_MEASURES_DTYPE
from spark_constants import CONNECTION_STR, CONNECTION_OBJECT, CONNECTION_PROPERTIES

def get_batches(connection_object):
    conn = psycopg2.connect(
        host=connection_object['host'],
        database=connection_object['database'],
        user=connection_object['user'],
        password=connection_object['password'])

    cursor = conn.cursor()

    cursor.execute("SELECT MAX(batch_number) FROM source")
    max_batch_num = cursor.fetchone()[0]

    if max_batch_num is None:
        # No batch
        return []
    else:
        batches = []
        org_keys = []
        servers = []
        # Per batch
        for i in range(max_batch_num + 1):
            cursor.execute(f"SELECT sonar_org_key, jenkins_server FROM source WHERE batch_number = {i}")
            for e in cursor.fetchall():
                org_keys.append(e[0])
                servers.append(e[1])
            batches.append((i, org_keys, servers))
        return batches

def get_data_from_db(spark, table, processed, custom_filter=[], org_server_filter_elements=None, all_columns=False):

    if table == "jenkins_builds":
        dtype = JENKINS_BUILD_DTYPE
        org_server_filter = "server"
    elif table == "sonar_analyses":
        dtype = SONAR_ANALYSES_DTYPE
        org_server_filter = "organization"
    elif table == "sonar_issues":
        dtype = SONAR_ISSUES_DTYPE
        org_server_filter = "organization"
    elif table == "sonar_measures":
        dtype = SONAR_MEASURES_DTYPE
        org_server_filter = "organization"

    if all_columns:
        columns = "*"
    else:    
        columns = ", ".join(dtype.keys())
    
    filters = []

    # Filter accumulation
    if org_server_filter_elements is not None:
        filters.append(f"""
            {org_server_filter} IN ({"'" + "', '".join(org_server_filter_elements) + "'"})
            """)

    if processed is not None:
        filters.append(f"processed is {processed}")

    filters += custom_filter

    filter_clause = "" if not filters else "WHERE " + " AND ".join(filters)
        
    query = f"SELECT {columns} FROM {table} " + filter_clause

    df = spark.read \
        .format("jdbc") \
        .option("url", CONNECTION_STR) \
        .option("user", CONNECTION_PROPERTIES["user"]) \
        .option("password", CONNECTION_PROPERTIES["password"]) \
        .option("query", query)\
        .load()

    return df