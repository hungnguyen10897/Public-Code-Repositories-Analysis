# pyspark --driver-class-path postgresql-42.2.12.jar
# spark-submit --driver-class-path postgresql-42.2.12.jar spark_project.py

from pyspark import SparkContext, SparkConf
from pyspark.sql import DataFrame, SparkSession
from pathlib import Path
import time, sys

from spark_constants import *
from pyspark.ml.stat import ChiSquareTest
from pyspark.ml.feature import ChiSqSelector

from model_3 import prepare_data_ml3

conf = SparkConf().setMaster('local[*]')
sc = SparkContext
spark = SparkSession.builder.config(conf = conf).getOrCreate()

def get_schema(source):
    if source == "jenkins builds":
        DTYPE = JENKINS_BUILD_DTYPE
    elif source == "jenkins tests":
        DTYPE = JENKINS_TEST_DTYPE
    elif source == "sonar measures":
        DTYPE = SONAR_MEASURES_DTYPE
    elif source == "sonar analyses":
        DTYPE = SONAR_ANALYSES_DTYPE
    elif source == "sonar issues":
        DTYPE = SONAR_ISSUES_DTYPE            

    field = []
    for col,type in DTYPE.items():
        if col in ['date', 'last_commit_date', 'commit_ts', 'creation_date', 'update_date', 'close_date']:
            field.append(StructField(col, TimestampType(), True))
        elif type == 'object':
            field.append(StructField(col, StringType(), True))
        elif type == 'Int64':
            field.append(StructField(col, LongType(), True))
        elif type == 'float64':
            field.append(StructField(col, DoubleType(), True))

    return StructType(field)

def issue_impact_process(ml_df, columns, project):

    r = ChiSquareTest.test(ml_df, "features", "label")
    pValues = r.select("pvalues").collect()[0][0].tolist()
    stats = r.select("statistics").collect()[0][0].tolist()
    dof = r.select("degreesOfFreedom").collect()[0][0]

    # ChiSq Selector
    selector =ChiSqSelector(numTopFeatures= 10, featuresCol="features", outputCol="selected_features", labelCol="label")
    selector_model = selector.fit(ml_df)       

    top_10_feaures_importance = []
    for j in selector_model.selectedFeatures:
        top_10_feaures_importance.append(columns[j])
        top_10_feaures_importance.append(stats[j])

    top_issue = [project, "ChiSquareSelectorModel", ml_df.count()] + top_10_feaures_importance
    top_issue_df = spark.createDataFrame(data = [top_issue], schema = TOP_ISSUE_SCHEMA)
    top_issue_df.write.jdbc(CONNECTION_STR, 'top_issues', mode='append', properties=CONNECTION_PROPERTIES)

def main(spark_artefacts_dir):
    
    project_jenkins_builds = spark.read.csv("./test_data/jmeter_jenkins_builds.csv", sep=',', schema = get_schema("jenkins builds"), ignoreLeadingWhiteSpace = True, 
        ignoreTrailingWhiteSpace = True, header=True, mode = 'FAILFAST')

    sonar_analyses = spark.read.csv("./test_data/jmeter_sonar_analyses.csv", sep=',', schema = get_schema("sonar analyses"), ignoreLeadingWhiteSpace = True, 
        ignoreTrailingWhiteSpace = True, header=True, mode = 'FAILFAST')

    sonar_issues = spark.read.csv("./test_data/jmeter_sonar_issues.csv", sep=',', schema = get_schema("sonar issues"), ignoreLeadingWhiteSpace = True, 
        ignoreTrailingWhiteSpace = True, header=True, mode = 'FAILFAST')    

    # sonar_analyses = spark.read.jdbc(CONNECTION_STR, "sonar_analyses", properties=CONNECTION_PROPERTIES)
    # sonar_analyses.persist()

    # sonar_issues = spark.read.jdbc(CONNECTION_STR, "sonar_issues", properties=CONNECTION_PROPERTIES)
    # sonar_issues.persist()

    projects = list(map(lambda x: x.project,sonar_analyses.select("project").distinct().collect()))

    for project in projects:

        project_sonar_analyses = sonar_analyses.filter(sonar_analyses.project == project)
        project_sonar_issues = sonar_issues.filter(sonar_issues.project == project)

        # project_jenkins_builds_query = f"""
        #     SELECT * FROM jenkins_builds WHERE revision_number IN (
        #         SELECT revision FROM sonar_analyses WHERE project = '{project}'
        #     )
        # """
        # project_jenkins_builds = spark.read \
        #     .format("jdbc") \
        #     .option("url", CONNECTION_STR) \
        #     .option("user", CONNECTION_PROPERTIES["user"]) \
        #     .option("password", CONNECTION_PROPERTIES["password"]) \
        #     .option("query", project_jenkins_builds_query)\
        #     .load()

        ml_df, columns = prepare_data_ml3(spark, project_jenkins_builds, project_sonar_issues, project_sonar_analyses, spark_artefacts_dir, "incremental")

        if ml_df is not None and ml_df.count() > 0:
            print(f"\tCount: {ml_df.count()}")
            issue_impact_process(ml_df, columns, project)
        else:
            print("\tNo data.")

if __name__ == "__main__":
    print("Start Spark Processing per Project.")

    spark_artefacts_dir = "./spark_artefacts"
    artefacts_path = Path(spark_artefacts_dir)
    if not artefacts_path.exists():
        print(f"Path to spark_artefacts at '{artefacts_path.absolute()}' does not exists(). Cancelling!")
        sys.exit(0)

    then = time.time()
    main(spark_artefacts_dir)
    print(f"Time elapsed: {time.time() - then}")