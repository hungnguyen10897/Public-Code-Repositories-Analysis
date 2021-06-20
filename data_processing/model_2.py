# ML model 2

from pyspark.ml.feature import StringIndexer, VectorAssembler, MinMaxScaler
from pyspark.ml import Pipeline, PipelineModel
from pyspark.sql.functions import udf

from spark_utils import *
from model_common import pipeline_process, feature_selector_process, train_predict

def get_ml2_pipeline():
    stages = []

    numerical_vector_assembler = VectorAssembler(inputCols=ML2_NUMERICAL_COLUMNS , outputCol="numerial_cols_vec", handleInvalid="keep")
    scaler = MinMaxScaler(inputCol="numerial_cols_vec", outputCol= "features")
    stages.append(numerical_vector_assembler)
    stages.append(scaler)

    label_str_indexer = StringIndexer(inputCol="result", outputCol="label", handleInvalid="keep")
    stages.append(label_str_indexer)

    pipeline = Pipeline(stages = stages)
    return pipeline

def prepare_data_ml2(spark, jenkins_builds, sonar_issues, sonar_analyses):

    with open('./sonar_issues_count.sql', 'r') as f:
        query1 = f.read()
    with open('./sonar_issues_count_with_current.sql', 'r') as f:
        query2 = f.read()

    sonar_issues.createOrReplaceTempView('sonar_issues')
    sonar_issues_count = spark.sql(query1)
    sonar_issues_count.createOrReplaceTempView('sonar_issues_count')
    sonar_issues_count_with_current = spark.sql(query2)
    sonar_df = sonar_issues_count_with_current.join(sonar_analyses, sonar_issues_count_with_current.analysis_key == sonar_analyses.analysis_key,
        how = "inner")
    df = sonar_df.join(jenkins_builds, sonar_df.revision == jenkins_builds.revision_number, how = "inner").select(*(['result'] + ML2_NUMERICAL_COLUMNS))

    # Change data types to fit in estimators
    for numerical_column in ML2_NUMERICAL_COLUMNS:
        df = df.withColumn(numerical_column, df[numerical_column].astype(DoubleType()))

    return df

def apply_ml2(spark, spark_artefacts_dir, run_mode):

    modify_result = udf(lambda x: "SUCCESS" if x == "SUCCESS" else "FAIL", StringType())
    spark.udf.register("modify_result" , modify_result)

    batches = get_batches(CONNECTION_OBJECT)
    if not batches:
        print("No batch to process!")
        return
    
    for i, (batch_num, org_keys, servers) in enumerate(batches):
        print(f"Processing batch {batch_num}")
        print(f"\tSonarcloud organization keys:\n\t\t" + "\n\t\t".join(org_keys))
        print(f"\tJenkins servers:\n\t\t" + "\n\t\t".join(servers))
        
        # Getting Data
        db_jenkins_builds = get_data_from_db(spark, "jenkins_builds", processed=None, org_server_filter_elements=servers, all_columns=True)
        db_sonar_analyses = get_data_from_db(spark, "sonar_analyses", processed=None, org_server_filter_elements=org_keys, all_columns=True, custom_filter=["analysis_key IS NOT NULL"])
        db_sonar_issues = get_data_from_db(spark, "sonar_issues", processed=None, org_server_filter_elements=org_keys, all_columns=True, custom_filter=["issue_key IS NOT NULL"]) 
        
        if run_mode == "incremental":
            unprocessed_jenkins_builds = db_jenkins_builds.filter(db_jenkins_builds["processed"] == False)
            unprocessed_sonar_analyses = db_sonar_analyses.filter(db_sonar_analyses["processed"] == False)
            unprocessed_sonar_issues = db_sonar_issues.filter(db_sonar_issues["processed"] == False)

        # Drop unused columns
        db_jenkins_builds = db_jenkins_builds.drop("server", "processed", "ingested_at").withColumn("result", modify_result("result"))
        db_sonar_analyses = db_sonar_analyses.drop("organization", "processed", "ingested_at")
        db_sonar_issues = db_sonar_issues.drop("organization", "processed", "ingested_at")

        if run_mode == "incremental":
            unprocessed_jenkins_builds = unprocessed_jenkins_builds.drop("server", "processed", "ingested_at").withColumn("result", modify_result("result"))
            unprocessed_sonar_analyses = unprocessed_sonar_analyses.drop("organization", "processed", "ingested_at")
            unprocessed_sonar_issues = unprocessed_sonar_issues.drop("organization", "processed", "ingested_at")

        # Prepare data
        if run_mode == "first":
            batch_df = prepare_data_ml2(spark, db_jenkins_builds, db_sonar_issues, db_sonar_analyses)

        elif run_mode == "incremental":
            # Unprocessed jenkins ~ db sonar
            df1 = prepare_data_ml2(spark, unprocessed_jenkins_builds, db_sonar_issues, db_sonar_analyses)
            # Unprocessed sonar ~ db jenkins
            df2 = prepare_data_ml2(spark, db_jenkins_builds, unprocessed_sonar_issues, db_sonar_analyses)
            batch_df = df1.union(df2)
                
        # Accumulation
        if i == 0:
            batches_df = batch_df
        else:
            batches_df = batches_df.union(batch_df)
    
    batches_df.persist()
    print(f"DF for ML2 Count: {str(batches_df.count())}")
    if batches_df.count() == 0:
        print("No data for ML2 - Returning...")
        return

    ml_df,_ = pipeline_process(batches_df, get_ml2_pipeline, spark_artefacts_dir, run_mode, 2)
    ml_df.persist()
    ml_df_10, ml_10_columns = feature_selector_process(spark, ml_df, spark_artefacts_dir, run_mode, 2, ML2_NUMERICAL_COLUMNS)
    ml_df_10.persist()

    train_predict(spark, ml_df, spark_artefacts_dir, run_mode, 2, ML2_NUMERICAL_COLUMNS)
    train_predict(spark, ml_df_10, spark_artefacts_dir, run_mode, 2, ml_10_columns, True)