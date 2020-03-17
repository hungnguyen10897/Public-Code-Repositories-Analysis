# pyspark --conf spark.executor.extraClassPath=sqlite-jdbc-3.30.1.jar --driver-class-path sqlite-jdbc-3.30.1.jar --jars sqlite-jdbc-3.30.1.jar
from pyspark.sql import SparkSession
from pathlib import Path
from pyspark.sql import DataFrame
from pyspark.sql.types import *
from pyspark import SparkContext

sc = SparkContext()
spark = SparkSession.builder.getOrCreate()

def retrieve_sqlite(sqlite_conn_url):
    db = {}

    projects_properties = {}
    projects_properties['customSchema'] = "projectID STRING, gitLink STRING, jiraLink STRING, sonarProjectKey STRING"
    projects_df = spark.read.jdbc(sqlite_conn_url, 'PROJECTS', properties = projects_properties)
    db['PROJECTS'] = projects_df

    refactoring_miner_properties = {}
    refactoring_miner_properties['customSchema'] = "projectID STRING, commitHash STRING, refactoringType STRING, refactoringDetail STRING"
    refactoring_miner_df = spark.read.jdbc(sqlite_conn_url, 'REFACTORING_MINER', properties = refactoring_miner_properties)
    db['REFACTORING_MINER'] = refactoring_miner_df

    return db

def get_jenkins_builds_data(jenkins_data_directory):
    
    data_path = Path(jenkins_data_directory)
    builds_path = data_path.joinpath('builds')

    field = [StructField("job",StringType(), True),
            StructField("build_number", IntegerType(), True),
            StructField("result", StringType(), True),
            StructField("duration", IntegerType(), True),
            StructField("estimated_duration", IntegerType(), True),
            StructField("revision_number", StringType(), True),
            StructField("commit_id", StringType(), True),
            StructField("commit_ts", TimestampType(), True),
            StructField("test_pass_count", IntegerType(), True),
            StructField("test_fail_count", IntegerType(), True),
            StructField("test_skip_count", IntegerType(), True),
            StructField("total_test_duration", DoubleType(), True)]

    schema = StructType(field)

    output = spark.createDataFrame(sc.emptyRDD(), schema)

    for file in builds_path.glob('*.csv'):
        df = spark.read.csv(file, sep=',', schema = schema, ignoreLeadingWhiteSpace = True, 
            ignoreTrailingWhiteSpace = True, header=True)

        output.union(df)

    output.withColumn('commit_ts', )

    return output

if __name__ == "__main__":

    jenkins_data_directory = "./jenkins_data/data"
    sqlite_conn_url = "jdbc:sqlite:/home/hung/MyWorksapce/BachelorThesis/SQLite-Database/technicalDebtDataset.db"

    db = retrieve_sqlite(sqlite_conn_url)
    projects_df = db['PROJECTS']

    jenkins_builds_df = get_jenkins_builds_data(jenkins_data_directory)
    jenkins_builds_df.show()


