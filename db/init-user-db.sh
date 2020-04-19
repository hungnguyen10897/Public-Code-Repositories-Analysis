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
		duration BIGINT,
		estimated_duration BIGINT,
		revision_number VARCHAR(50),
		commit_id VARCHAR(50),
		commit_ts TIMESTAMP WITHOUT TIME ZONE,
		test_pass_count INT,
		test_fail_count INT,
		test_skip_count INT,
		total_test_duration FLOAT,
		ingested_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
	);
	ALTER TABLE jenkins_builds OWNER to pra;

	CREATE TABLE jenkins_tests(
		job VARCHAR NOT NULL,
		build_number INT NOT NULL,
		package VARCHAR(50),
		class VARCHAR(50),
		name VARCHAR(50),
		duration INT,
		status VARCHAR(50),
		ingested_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
	);
	ALTER TABLE jenkins_tests OWNER TO pra;

	CREATE TABLE sonar_analyses(
		project VARCHAR NOT NULL, 
		analysis_key VARCHAR,
		date TIMESTAMP WITHOUT TIME ZONE,
		project_version VARCHAR,
		revision VARCHAR,
		ingested_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
	);
	ALTER TABLE sonar_analyses OWNER TO pra;

	CREATE TABLE sonar_measures(
		project VARCHAR NOT NULL,
		analysis_key VARCHAR,
		complexity INT,
		file_complexity FLOAT,
		cognitive_complexity INT,
		test_errors INT,
		skipped_tests INT,
		test_failures INT,
		tests INT,
		test_execution_time INT,
		test_success_density FLOAT,
		coverage FLOAT,
		lines_to_cover INT,
		uncovered_lines INT,
		line_coverage FLOAT,
		conditions_to_cover INT,
		uncovered_conditions INT,
		branch_coverage FLOAT,
		duplicated_lines INT,
		duplicated_lines_density FLOAT,
		duplicated_blocks INT,
		duplicated_files INT,
		quality_profiles VARCHAR,
		quality_gate_details VARCHAR,
		violations INT,
		blocker_violations INT,
		critical_violations INT,
		major_violations INT,
		minor_violations INT,
		info_violations INT,
		false_positive_issues INT,
		open_issues INT,
		reopened_issues INT,
		confirmed_issues INT,
		wont_fix_issues INT,
		sqale_index INT,
		sqale_rating FLOAT,
		development_cost FLOAT,
		sqale_debt_ratio FLOAT,
		new_sqale_debt_ratio FLOAT,
		code_smells INT,
		effort_to_reach_maintainability_rating_a INT,
		new_development_cost FLOAT,
		alert_status VARCHAR,
		bugs INT,
		reliability_remediation_effort INT,
		reliability_rating FLOAT,
		last_commit_date TIMESTAMP WITHOUT TIME ZONE,
		vulnerabilities INT,
		security_remediation_effort INT,
		security_rating FLOAT,
		security_hotspots INT,
		security_review_rating FLOAT,
		classes INT,
		ncloc INT,
		functions INT,
		comment_lines INT,
		comment_lines_density FLOAT,
		files INT,
		lines INT,
		statements INT,
		ncloc_language_distribution VARCHAR,
		ingested_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
	);
	ALTER TABLE sonar_measures OWNER TO pra;

	CREATE TABLE sonar_issues(
                project VARCHAR NOT NULL, 
                current_analysis_key VARCHAR,
		creation_analysis_key VARCHAR,
		issues_key VARCHAR,
		type VARCHAR(20),
		rule VARCHAR,
		severity VARCHAR(20),
		status VARCHAR(20),
		resolution VARCHAR(20),
		effort INT,
		debt INT,
		tags VARCHAR,
		creation_date TIMESTAMP WITHOUT TIME ZONE,
		update_date TIMESTAMP WITHOUT TIME ZONE,
		close_date TIMESTAMP WITHOUT TIME ZONE,
                ingested_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );
        ALTER TABLE sonar_issues OWNER TO pra;	

	CREATE TABLE model_performance(
		model VARCHAR(20) NOT NULL,
		data_amount INT,
		f1 FLOAT,
		weighted_precision FLOAT,
		weighted_recall FLOAT,
		accuracy FLOAT,
		processed_date DATE DEFAULT CURRENT_DATE 
	);
	ALTER TABLE model_performance OWNER TO pra;

	
EOSQL
