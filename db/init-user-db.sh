#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
	CREATE USER airflow;
        CREATE DATABASE airflow;
        GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;
	ALTER USER airflow WITH PASSWORD 'airflow';

        CREATE USER superset;
        CREATE DATABASE superset;
        GRANT ALL PRIVILEGES ON DATABASE superset TO superset;
        ALTER USER superset WITH PASSWORD 'superset';

	CREATE USER pra;
        CREATE DATABASE pra;
        GRANT ALL PRIVILEGES ON DATABASE pra TO pra;
        ALTER USER pra WITH PASSWORD 'pra';

	\c pra

	CREATE TABLE jenkins_builds(
		job VARCHAR(50) NOT NULL,
		build_number INT NOT NULL,
		result VARCHAR(50),
		duration INT,
		estimated_duration INT,
		revision_number VARCHAR(50),
		commit_id VARCHAR(50),
		commit_ts TIMESTAMP WITHOUT TIME ZONE,
		test_pass_count INT,
		test_fail_count INT,
		test_skip_count INT,
		total_test_duration FLOAT
	);
	ALTER TABLE jenkins_builds OWNER to pra;

	CREATE TABLE jenkins_tests(
		job VARCHAR(50) NOT NULL,
		build_number INT NOT NULL,
		package VARCHAR(50),
		class VARCHAR(50),
		name VARCHAR(50),
		duration INT,
		status VARCHAR(50)
	);
	ALTER TABLE jenkins_tests OWNER TO pra;


EOSQL
