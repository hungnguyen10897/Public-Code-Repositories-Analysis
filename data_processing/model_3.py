# ML model 3

from pyspark.ml.feature import StringIndexer, StringIndexerModel, VectorAssembler, OneHotEncoderEstimator
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.linalg import DenseVector, SparseVector
from pyspark.sql.functions import udf
from pyspark.sql.types import Row, StringType
from pathlib import Path
from collections import OrderedDict

from model_common import feature_selector_process, train_predict
from spark_utils import *

def get_ml3_pipeline():

    stages = []
    str_idx = StringIndexer(inputCol="rule", outputCol="rule_idx", handleInvalid="skip")
    ohe = OneHotEncoderEstimator(inputCols=["rule_idx"], outputCols=["rule_vec"], dropLast=False)
    stages = [str_idx, ohe]
    return Pipeline(stages= stages)

def sum_sparse_vectors(v1, v2):

    def add_to_map(m, e):
        key = e[0]
        value = e[1]
        if key in m:
            m[key] = m[key] + value
        else:
            m[key] = value
        return m
    results = OrderedDict()

    v = list(zip(v1.indices, v1.values)) + list(zip(v2.indices, v2.values))
    v.sort(key = lambda x: x[0])
    for e in v:
        results = add_to_map(results, e)
    return SparseVector(v1.size, list(results.keys()), list(results.values()))

def prepare_data_ml3(spark, jenkins_builds, sonar_issues, sonar_analyses , spark_artefacts_dir, run_mode):

    # Change build result to only SUCCESS/FAIL for binary classification
    modify_result = udf(lambda x: "SUCCESS" if x == "SUCCESS" else "FAIL", StringType())
    spark.udf.register("modify_result" , modify_result)

    if jenkins_builds is not None:
        jenkins_builds = jenkins_builds.withColumn("result", modify_result("result"))

    pipeline_path = Path(spark_artefacts_dir).joinpath("pipeline_3")
    label_idx_model_path = Path(spark_artefacts_dir).joinpath("label_indexer_3")

    # Getting pipeline and label indexer models
    if run_mode == "first":

        pipeline_model = get_ml3_pipeline().fit(sonar_issues)
        pipeline_model.write().overwrite().save(str(pipeline_path.absolute()))

        label_idx_model = StringIndexer(inputCol="result", outputCol="label", handleInvalid="skip").fit(jenkins_builds)
        label_idx_model.write().overwrite().save(str(label_idx_model_path.absolute()))

    elif run_mode == "incremental":

        pipeline_model = PipelineModel.load(str(pipeline_path.absolute()))
        label_idx_model = StringIndexerModel.load(str(label_idx_model_path.absolute()))

    # Columns to return
    rules = pipeline_model.stages[0].labels
    columns = list(map(lambda x: "removed_" + x, rules)) + list(map(lambda x: "introduced_" + x, rules))

    # Preparing
    removed_rules_df = sonar_issues.filter("status IN ('RESOLVED', 'CLOSED', 'REVIEWED')").select("current_analysis_key","rule")

    df1 = pipeline_model.transform(removed_rules_df)
    rdd1 = df1.rdd.map(lambda x : (x[0],x[3])).reduceByKey(lambda v1,v2: sum_sparse_vectors(v1,v2)) \
                                                .map(lambda x: Row(current_analysis_key = x[0], removed_rule_vec = x[1]))
    
    if rdd1.count() == 0:
        return None, columns
    removed_issues_rule_vec_df = spark.createDataFrame(rdd1)

    introduced_rules_df = sonar_issues.filter("status IN ('OPEN', 'REOPENED', 'CONFIRMED', 'TO_REVIEW')").select("creation_analysis_key","rule")
    df2 = pipeline_model.transform(introduced_rules_df)
    rdd2 = df2.rdd.map(lambda x : (x[0],x[3])).reduceByKey(lambda v1,v2: sum_sparse_vectors(v1,v2)) \
                                                .map(lambda x: Row(creation_analysis_key = x[0], introduced_rule_vec = x[1]))
                                            
    if rdd2.count() == 0:
        return None, columns                                       
    introduced_issues_rule_vec_df = spark.createDataFrame(rdd2)

    joined_sonar_rules_df = removed_issues_rule_vec_df.join(introduced_issues_rule_vec_df, 
        removed_issues_rule_vec_df.current_analysis_key == introduced_issues_rule_vec_df.creation_analysis_key, how = "outer")

    joined_sonar_rules_df.createOrReplaceTempView("sonar_rules")
    joined_sonar_rules_df = spark.sql("""SELECT 
        coalesce(current_analysis_key, creation_analysis_key) AS analysis_key,
        introduced_rule_vec,
        removed_rule_vec
        FROM sonar_rules
    """)

    num_rules = len(pipeline_model.stages[0].labels)

    imputed_sonar_rules_rdd = joined_sonar_rules_df.rdd.map(lambda row: Row(
        analysis_key = row[0], 
        introduced_rule_vec = SparseVector(num_rules,{}) if row[1] is None else row[1], 
        removed_rule_vec = SparseVector(num_rules,{}) if row[2] is None else row[2]))

    imputed_sonar_rules_df = spark.createDataFrame(imputed_sonar_rules_rdd)        
    
    v_assembler = VectorAssembler(inputCols=["removed_rule_vec", "introduced_rule_vec"], outputCol="features")
    sonar_issues_df = v_assembler.transform(imputed_sonar_rules_df).select("analysis_key","features")

    sonar_df = sonar_issues_df.join(sonar_analyses, sonar_issues_df.analysis_key == sonar_analyses.analysis_key, how = "inner")
    df = sonar_df.join(jenkins_builds, sonar_df.revision == jenkins_builds.revision_number, how = "inner").select("result", "features")
    ml_df = label_idx_model.transform(df).select("label", "features")

    return ml_df, columns

def apply_ml3(spark, spark_artefacts_dir, run_mode):

    # Getting Data
    db_jenkins_builds = get_data_from_db(spark, "jenkins_builds", processed=None, all_columns=True)
    db_sonar_analyses = get_data_from_db(spark, "sonar_analyses", processed=None, all_columns=True, custom_filter=["analysis_key IS NOT NULL"])
    db_sonar_issues = get_data_from_db(spark, "sonar_issues", processed=None, all_columns=True, custom_filter=["issue_key IS NOT NULL"]) 
    
    if run_mode == "incremental":
        unprocessed_jenkins_builds = db_jenkins_builds.filter(db_jenkins_builds["processed"] == False)
        unprocessed_sonar_analyses = db_sonar_analyses.filter(db_sonar_analyses["processed"] == False)
        unprocessed_sonar_issues = db_sonar_issues.filter(db_sonar_issues["processed"] == False)

    # Drop unused columns
    db_jenkins_builds = db_jenkins_builds.drop("server", "processed", "ingested_at")
    db_sonar_analyses = db_sonar_analyses.drop("organization", "processed", "ingested_at")
    db_sonar_issues = db_sonar_issues.drop("organization", "processed", "ingested_at")

    if run_mode == "incremental":
        unprocessed_jenkins_builds = unprocessed_jenkins_builds.drop("server", "processed", "ingested_at")
        unprocessed_sonar_analyses = unprocessed_sonar_analyses.drop("organization", "processed", "ingested_at")
        unprocessed_sonar_issues = unprocessed_sonar_issues.drop("organization", "processed", "ingested_at")

    if run_mode == "first":

        ml_df, columns = prepare_data_ml3(spark, db_jenkins_builds, db_sonar_issues, db_sonar_analyses, spark_artefacts_dir, run_mode)
        if ml_df is None:
            print("No data for ML3 - Returning...")
            return

    elif run_mode == "incremental":
        
        df1, columns = prepare_data_ml3(spark, unprocessed_jenkins_builds, db_sonar_issues, db_sonar_analyses, spark_artefacts_dir, run_mode)
        df2, columns = prepare_data_ml3(spark, db_jenkins_builds, unprocessed_sonar_issues, db_sonar_analyses, spark_artefacts_dir, run_mode)
        
        if df1 is None and df2 is None:
            print("No data for ML3 - Returning...")
            return
        elif df1 is None:
            ml_df = df2
        elif df2 is None:
            ml_df = df1
        else:
            ml_df = df1.union(df2)

    ml_df.persist()
    print(f"DF for ML3 Count: {ml_df.count()}")
    if ml_df.count() == 0:
        print("No data for ML3 - Returning...")
        return

    ml_df_10, ml_10_columns = feature_selector_process(spark, ml_df, spark_artefacts_dir, run_mode, 3, columns)
    ml_df_10.persist()

    train_predict(spark, ml_df, spark_artefacts_dir, run_mode, 3, columns)
    train_predict(spark, ml_df_10, spark_artefacts_dir, run_mode, 3, ml_10_columns, True)