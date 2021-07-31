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

	CREATE TABLE source(
		id SERIAL,
 		sonar_org_key  VARCHAR,
 		jenkins_server  VARCHAR,
 		batch_number  integer,
 		input_date DATE DEFAULT CURRENT_DATE
	);
	ALTER TABLE source OWNER TO pra;

	INSERT INTO source (sonar_org_key, jenkins_server, batch_number) VALUES ('apache', 'https://builds.apache.org/', 0);

	CREATE TABLE jenkins_builds(
		server VARCHAR NOT NULL,
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
		processed BOOLEAN DEFAULT FALSE,
		ingested_at DATE DEFAULT CURRENT_DATE
	);
	ALTER TABLE jenkins_builds OWNER TO pra;

	CREATE TABLE sonar_analyses(
		organization VARCHAR NOT NULL,
		project VARCHAR NOT NULL, 
		analysis_key VARCHAR,
		date TIMESTAMP WITHOUT TIME ZONE,
		project_version VARCHAR,
		revision VARCHAR,
		processed BOOLEAN DEFAULT FALSE,
		ingested_at DATE DEFAULT CURRENT_DATE
	);
	ALTER TABLE sonar_analyses OWNER TO pra;

	CREATE TABLE sonar_measures(
		organization VARCHAR NOT NULL,
		project VARCHAR NOT NULL,
		analysis_key VARCHAR,
		complexity INT,
		class_complexity FLOAT,
		function_complexity FLOAT,
		file_complexity FLOAT,
		function_complexity_distribution VARCHAR,
		file_complexity_distribution VARCHAR,
		complexity_in_classes INT,
		complexity_in_functions INT,
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
		new_coverage FLOAT,
		new_lines_to_cover INT,
		new_uncovered_lines INT,
		new_line_coverage FLOAT,
		new_conditions_to_cover INT,
		new_uncovered_conditions INT,
		new_branch_coverage FLOAT,
		executable_lines_data VARCHAR,
		public_api INT,
		public_documented_api_density FLOAT,
		public_undocumented_api INT,
		duplicated_lines INT,
		duplicated_lines_density FLOAT,
		duplicated_blocks INT,
		duplicated_files INT,
		duplications_data VARCHAR,
		new_duplicated_lines INT,
		new_duplicated_blocks INT,
		new_duplicated_lines_density FLOAT,
		quality_profiles VARCHAR,
		quality_gate_details VARCHAR,
		violations INT,
		blocker_violations INT,
		critical_violations INT,
		major_violations INT,
		minor_violations INT,
		info_violations INT,
		new_violations INT,
		new_blocker_violations INT,
		new_critical_violations INT,
		new_major_violations INT,
		new_minor_violations INT,
		new_info_violations INT,
		false_positive_issues INT,
		open_issues INT,
		reopened_issues INT,
		confirmed_issues INT,
		wont_fix_issues INT,
		sqale_index INT,
		sqale_rating FLOAT,
		development_cost FLOAT,
		new_technical_debt INT,
		sqale_debt_ratio FLOAT,
		new_sqale_debt_ratio FLOAT,
		code_smells INT,
		new_code_smells INT,
		effort_to_reach_maintainability_rating_a INT,
		new_maintainability_rating FLOAT,
		new_development_cost FLOAT,
		alert_status VARCHAR,
		bugs INT,
		new_bugs INT,
		reliability_remediation_effort INT,
		new_reliability_remediation_effort INT,
		reliability_rating FLOAT,
		new_reliability_rating FLOAT,
		last_commit_date TIMESTAMP WITHOUT TIME ZONE,
		vulnerabilities INT,
		new_vulnerabilities INT,
		security_remediation_effort INT,
		new_security_remediation_effort INT,
		security_rating FLOAT,
		new_security_rating FLOAT,
		security_hotspots INT,
		new_security_hotspots INT,
		security_review_rating FLOAT,
		classes INT,
		ncloc INT,
		functions INT,
		comment_lines INT,
		comment_lines_density FLOAT,
		files INT,
		directories INT,
		lines INT,
		statements INT,
		generated_lines INT,
		generated_ncloc INT,
		ncloc_data VARCHAR,
		comment_lines_data VARCHAR,
		projects INT,
		ncloc_language_distribution VARCHAR,
		new_lines INT,
		processed BOOLEAN DEFAULT FALSE,
		ingested_at DATE DEFAULT CURRENT_DATE
	);
	ALTER TABLE sonar_measures OWNER TO pra;

	CREATE TABLE sonar_issues(
		organization VARCHAR NOT NULL,
		project VARCHAR NOT NULL, 
		current_analysis_key VARCHAR,
		creation_analysis_key VARCHAR,
		issue_key VARCHAR,
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
		processed BOOLEAN DEFAULT FALSE,
        ingested_at DATE DEFAULT CURRENT_DATE
    );
    ALTER TABLE sonar_issues OWNER TO pra;	

	CREATE TABLE model_performance(
		model VARCHAR NOT NULL,
		data_amount INT,
		f1 FLOAT,
		weighted_precision FLOAT,
		weighted_recall FLOAT,
		accuracy FLOAT,
		area_under_ROC FLOAT,
		area_under_PR FLOAT,
		predicted_negative_rate FLOAT,
		processing_date DATE DEFAULT CURRENT_DATE 
	);
	ALTER TABLE model_performance OWNER TO pra;

	CREATE TABLE model_info(
		model VARCHAR NOT NULL,
		train_data_amount INT,
		train_f1 FLOAT,
		train_weighted_precision FLOAT,
		train_weighted_recall FLOAT,
		train_accuracy FLOAT,
		train_area_under_ROC FLOAT,
		train_area_under_PR FLOAT,
		train_predicted_negative_rate FLOAT,
		feature_1 VARCHAR,
		feature_importance_1 FLOAT,
		feature_2 VARCHAR,
		feature_importance_2 FLOAT,
		feature_3 VARCHAR,
		feature_importance_3 FLOAT,
		feature_4 VARCHAR,
		feature_importance_4 FLOAT,
		feature_5 VARCHAR,
		feature_importance_5 FLOAT,
		feature_6 VARCHAR,
		feature_importance_6 FLOAT,
		feature_7 VARCHAR,
		feature_importance_7 FLOAT,
		feature_8 VARCHAR,
		feature_importance_8 FLOAT,
		feature_9 VARCHAR,
		feature_importance_9 FLOAT,
		feature_10 VARCHAR,
		feature_importance_10 FLOAT,
		processing_date DATE DEFAULT CURRENT_DATE	
	);
	ALTER TABLE model_info OWNER TO pra;

	CREATE TABLE top_issues(
		organization VARCHAR NOT NULL,
		project VARCHAR NOT NULL,
		model VARCHAR NOT NULL,
		input_data_amount INT,
		issue_1 VARCHAR,
		issue_importance_1 FLOAT,
		issue_2 VARCHAR,
		issue_importance_2 FLOAT,
		issue_3 VARCHAR,
		issue_importance_3 FLOAT,
		issue_4 VARCHAR,
		issue_importance_4 FLOAT,
		issue_5 VARCHAR,
		issue_importance_5 FLOAT,
		issue_6 VARCHAR,
		issue_importance_6 FLOAT,
		issue_7 VARCHAR,
		issue_importance_7 FLOAT,
		issue_8 VARCHAR,
		issue_importance_8 FLOAT,
		issue_9 VARCHAR,
		issue_importance_9 FLOAT,
		issue_10 VARCHAR,
		issue_importance_10 FLOAT,
		processing_date DATE DEFAULT CURRENT_DATE
	);
	ALTER TABLE top_issues OWNER TO pra;

EOSQL
