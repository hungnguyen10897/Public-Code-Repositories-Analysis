# pyspark --conf spark.executor.extraClassPath=sqlite-jdbc-3.30.1.jar --driver-class-path sqlite-jdbc-3.30.1.jar --jars sqlite-jdbc-3.30.1.jar
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

properties = {}
properties['customSchema'] = "projectID STRING, gitLink STRING, jiraLink STRING, sonarProjectKey STRING"
sqlite_df = spark.read.jdbc('jdbc:sqlite:/home/hung/MyWorksapce/BachelorThesis/SQLite-Database/technicalDebtDataset.db', 'PROJECTS', properties = properties)