import psycopg2

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

