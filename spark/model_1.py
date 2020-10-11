# ML model 1

from pyspark.ml.feature import OneHotEncoderEstimator, StringIndexer, StringIndexerModel, VectorAssembler, MinMaxScaler, Imputer, ChiSqSelector, ChiSqSelectorModel
from pyspark.ml import Pipeline, PipelineModel
from pyspark.sql.functions import udf

from spark_constants import *
from common import pipeline_process, feature_selector_process, train_predict, get_categorical_columns

def get_ml1_pipeline():
    stages = []

    imputer = Imputer(inputCols=ML1_NUMERICAL_COLUMNS , outputCols=ML1_NUMERICAL_COLUMNS )
    stages.append(imputer)

    ohe_input_cols = []
    ohe_output_cols = []
    for categorical_column in ML1_CATEGORICAL_COLUMNS:
        str_indexer = StringIndexer(inputCol=categorical_column, outputCol=categorical_column + "_index", handleInvalid='keep')
        ohe_input_cols.append(str_indexer.getOutputCol())
        ohe_output_cols.append(categorical_column + "_class_vec")
        stages.append(str_indexer)

    encoder = OneHotEncoderEstimator(inputCols=ohe_input_cols, outputCols=ohe_output_cols, handleInvalid="error", dropLast=False)
    stages.append(encoder)

    numerical_vector_assembler = VectorAssembler(inputCols=ML1_NUMERICAL_COLUMNS , outputCol="numerial_cols_vec", handleInvalid="keep")
    scaler = MinMaxScaler(inputCol="numerial_cols_vec", outputCol= "scaled_numerical_cols")
    stages.append(numerical_vector_assembler)
    stages.append(scaler)

    label_str_indexer = StringIndexer(inputCol="result", outputCol="label", handleInvalid="keep")
    stages.append(label_str_indexer)

    assembler_input = encoder.getOutputCols() + [scaler.getOutputCol()]
    assembler = VectorAssembler(inputCols= assembler_input, outputCol="features", handleInvalid="skip")
    stages.append(assembler)

    pipeline = Pipeline(stages = stages)
    return pipeline

def prepare_data_ml1(jenkins_builds, sonar_measures, sonar_analyses):

    ml_sonar_df = sonar_measures.join(sonar_analyses, sonar_measures.analysis_key == sonar_analyses.analysis_key, 
    how = 'inner').select(*(['revision'] + SONAR_MEASURES_NUMERICAL_COLUMNS + SONAR_MEASURES_CATEGORICAL_COLUMNS))
    df = jenkins_builds.join(ml_sonar_df, jenkins_builds.revision_number == ml_sonar_df.revision, how = 'inner')

    # Change data type from Int to Float to fit into estimators
    for column_name in ML1_NUMERICAL_COLUMNS:
        if column_name in JENKINS_BUILD_DTYPE:
            if JENKINS_BUILD_DTYPE[column_name] == 'Int64':
                df = df.withColumn(column_name, df[column_name].astype(DoubleType()))
        elif column_name in SONAR_MEASURES_DTYPE:
            if SONAR_MEASURES_DTYPE[column_name] == 'Int64':
                df = df.withColumn(column_name, df[column_name].astype(DoubleType()))
    return df

def apply_ml1(spark, new_jenkins_builds, db_jenkins_builds, new_sonar_measures, db_sonar_measures, new_sonar_analyses, db_sonar_analyses, spark_artefacts_dir, run_mode):

    modify_result = udf(lambda x: "SUCCESS" if x == "SUCCESS" else "FAIL", StringType())
    spark.udf.register("modify_result" , modify_result)

    if new_jenkins_builds is not None:
        new_jenkins_builds = new_jenkins_builds.withColumn("result", modify_result("result"))

    if db_jenkins_builds is not None:
        db_jenkins_builds = db_jenkins_builds.withColumn("result", modify_result("result"))

    # PREPARE DATA
    if run_mode == "first":
        df = prepare_data_ml1(new_jenkins_builds, new_sonar_measures, new_sonar_analyses)

    elif run_mode == "incremental":
        # New jenkins ~ db sonar
        df1 = prepare_data_ml1(new_jenkins_builds, db_sonar_measures, db_sonar_analyses)
        # New sonar ~ db jenkins
        df2 = prepare_data_ml1(db_jenkins_builds, new_sonar_measures, db_sonar_analyses)

        df = df1.union(df2).drop_duplicates()

    df.persist()  
    print(f"DF for ML1 Count: {str(df.count())}")
    if df.count() == 0:
        print("No data for ML1 - Returning...")
        return

    ml_df, pipeline_model = pipeline_process(df, get_ml1_pipeline, spark_artefacts_dir, run_mode, 1)
    ml_df.persist()
    global ML1_COLUMNS
    ML1_COLUMNS = get_categorical_columns(pipeline_model.stages[2].categorySizes) + ML1_NUMERICAL_COLUMNS
    ml_df_10, ml_10_columns = feature_selector_process(spark, ml_df, spark_artefacts_dir, run_mode, 1, ML1_COLUMNS)
    ml_df_10.persist()

    train_predict(spark, ml_df, spark_artefacts_dir, run_mode, 1, ML1_COLUMNS)
    train_predict(spark, ml_df_10, spark_artefacts_dir, run_mode, 1, ml_10_columns, True)
