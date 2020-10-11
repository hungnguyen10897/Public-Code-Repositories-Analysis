# pyspark --driver-class-path postgresql-42.2.12.jar
# spark-submit --driver-class-path postgresql-42.2.12.jar spark.py

from pyspark import SparkContext, SparkConf
from pyspark.sql import DataFrame, SparkSession
from pathlib import Path

from spark_constants import *
from spark import get_data_from_file
from pyspark.ml import Pipeline, PipelineModel

conf = SparkConf().setMaster('local[*]')
sc = SparkContext
spark = SparkSession.builder.config(conf = conf).getOrCreate()

def ml3():
    pass

def main(spark_artefacts_dir):
    db_jenkins_builds = spark.read.jdbc(CONNECTION_STR, "jenkins_builds", properties=CONNECTION_PROPERTIES)
    db_sonar_analyses = spark.read.jdbc(CONNECTION_STR, "sonar_analyses", properties=CONNECTION_PROPERTIES) 
    db_sonar_measures = spark.read.jdbc(CONNECTION_STR, "sonar_measures", properties=CONNECTION_PROPERTIES) 

    projects = list(map(lambda x: x.project,db_sonar_analyses.select("project").distinct().collect()))
    for project in projects:
        project_sonar_analyses = db_sonar_analyses.filter(db_sonar_analyses.project == project)
        project_analysis_keys = project_sonar_analyses.select("analysis_key").collect()
        project

    pipeline_path = Path(spark_artefacts_dir).joinpath(f"pipeline_3")
    pipeline_model = PipelineModel.load(str(pipeline_path.absolute()))

if __name__ == "__main__":
    spark_artefacts_dir = "./spark_artefacts"
    main(spark_artefacts_dir)