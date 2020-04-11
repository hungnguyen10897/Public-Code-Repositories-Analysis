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
		job VARCHAR NOT NULL,
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
		job VARCHAR NOT NULL,
		build_number INT NOT NULL,
		package VARCHAR(50),
		class VARCHAR(50),
		name VARCHAR(50),
		duration INT,
		status VARCHAR(50)
	);
	ALTER TABLE jenkins_tests OWNER TO pra;

	CREATE TABLE sonarqube(
		project VARCHAR NOT NULL,
		version VARCHAR,
		date  VARCHAR,
		revision VARCHAR,
		complexity INT,
		class_complexity VARCHAR,
		function_complexity VARCHAR,
		file_complexity FLOAT,
		function_complexity_distribution VARCHAR,
		file_complexity_distribution VARCHAR,
		complexity_in_classes VARCHAR,
		complexity_in_functions VARCHAR,
		cognitive_complexity INT,
		test_errors INT,
		skipped_tests INT,
		test_failures INT,
		tests INT,
		test_execution_time VARCHAR,
		test_success_density FLOAT,
		coverage FLOAT,
		lines_to_cover INT,
		uncovered_lines INT,
		line_coverage FLOAT,
		conditions_to_cover INT,
		uncovered_conditions INT,
		branch_coverage FLOAT,
		new_coverage VARCHAR,
		new_lines_to_cover VARCHAR,
		new_uncovered_lines VARCHAR,
		new_line_coverage VARCHAR,
		new_conditions_to_cover VARCHAR,
		new_uncovered_conditions VARCHAR,
		new_branch_coverage VARCHAR,
		executable_lines_data VARCHAR,
		public_api VARCHAR,
		public_documented_api_density VARCHAR,
		public_undocumented_api VARCHAR,
		duplicated_lines INT,
		duplicated_lines_density FLOAT,
		duplicated_blocks INT,
		duplicated_files INT,
		duplications_data VARCHAR,
		new_duplicated_lines VARCHAR,
		new_duplicated_blocks VARCHAR,
		new_duplicated_lines_density VARCHAR,
		quality_profiles VARCHAR,
		quality_gate_details VARCHAR,
		violations INT,
		blocker_violations INT,
		critical_violations INT,
		major_violations INT,
		minor_violations INT,
		info_violations INT,
		new_violations VARCHAR,
		new_blocker_violations VARCHAR,
		new_critical_violations VARCHAR,
		new_major_violations VARCHAR,
		new_minor_violations VARCHAR,
		new_info_violations VARCHAR,
		false_positive_issues INT,
		open_issues INT,
		reopened_issues INT,
		confirmed_issues INT,
		wont_fix_issues INT,
		sqale_index INT,
		sqale_rating FLOAT,
		development_cost VARCHAR,
		new_technical_debt VARCHAR,
		sqale_debt_ratio FLOAT,
		new_sqale_debt_ratio FLOAT,
		code_smells INT,
		new_code_smells VARCHAR,
		effort_to_reach_maintainability_rating_a INT,
		new_maintainability_rating VARCHAR,
		new_development_cost FLOAT,
		alert_status VARCHAR,
		bugs INT,
		new_bugs VARCHAR,
		reliability_remediation_effort INT,
		new_reliability_remediation_effort VARCHAR,
		reliability_rating FLOAT,
		new_reliability_rating VARCHAR,
		last_commit_date VARCHAR,
		vulnerabilities INT,
		new_vulnerabilities VARCHAR,
		security_remediation_effort INT,
		new_security_remediation_effort VARCHAR,
		security_rating FLOAT,
		new_security_rating VARCHAR,
		security_hotspots INT,
		new_security_hotspots VARCHAR,
		security_review_rating FLOAT,
		classes INT,
		ncloc INT,
		functions INT,
		comment_lines INT,
		comment_lines_density FLOAT,
		files INT,
		directories VARCHAR,
		lines INT,
		statements INT,
		generated_lines VARCHAR,
		generated_ncloc VARCHAR,
		ncloc_data VARCHAR,
		comment_lines_data VARCHAR,
		projects VARCHAR,
		ncloc_language_distribution VARCHAR,
		new_lines VARCHAR
	);
	ALTER TABLE sonarqube OWNER TO pra;

EOSQL
