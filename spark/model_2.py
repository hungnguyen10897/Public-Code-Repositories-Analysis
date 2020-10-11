# ML model 2

from pyspark.ml.feature import StringIndexer, VectorAssembler, MinMaxScaler
from pyspark.ml import Pipeline, PipelineModel
from pyspark.sql.functions import udf

from spark_constants import *
from common import pipeline_process, feature_selector_process, train_predict

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

def apply_ml2(spark, new_jenkins_builds, db_jenkins_builds, new_sonar_issues, db_sonar_issues,  new_sonar_analyses, db_sonar_analyses, spark_artefacts_dir, run_mode):

    modify_result = udf(lambda x: "SUCCESS" if x == "SUCCESS" else "FAIL", StringType())
    spark.udf.register("modify_result" , modify_result)

    if new_jenkins_builds is not None:
        new_jenkins_builds = new_jenkins_builds.withColumn("result", modify_result("result"))

    if db_jenkins_builds is not None:
        db_jenkins_builds = db_jenkins_builds.withColumn("result", modify_result("result"))

    if run_mode == "first":
        df = prepare_data_ml2(spark, new_jenkins_builds, new_sonar_issues, new_sonar_analyses)

    elif run_mode == "incremental":

        # New jenkins ~ db sonar
        df1 = prepare_data_ml2(spark, new_jenkins_builds, db_sonar_issues, db_sonar_analyses)

        # New sonar ~ db jenkins
        df2 = prepare_data_ml2(spark, db_jenkins_builds, new_sonar_issues, db_sonar_analyses)

        df = df1.union(df2)
    
    df.persist()
    print(f"DF for ML2 Count: {str(df.count())}")
    if df.count() == 0:
        print("No data for ML2 - Returning...")
        return

    ml_df,_ = pipeline_process(df, get_ml2_pipeline, spark_artefacts_dir, run_mode, 2)
    ml_df.persist()
    ml_df_10, ml_10_columns = feature_selector_process(spark, ml_df, spark_artefacts_dir, run_mode, 2, ML2_NUMERICAL_COLUMNS)
    ml_df_10.persist()

    train_predict(spark, ml_df, spark_artefacts_dir, run_mode, 2, ML2_NUMERICAL_COLUMNS)
    train_predict(spark, ml_df_10, spark_artefacts_dir, run_mode, 2, ml_10_columns, True)