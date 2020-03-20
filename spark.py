# pyspark --conf spark.executor.extraClassPath=sqlite-jdbc-3.30.1.jar --driver-class-path sqlite-jdbc-3.30.1.jar --jars sqlite-jdbc-3.30.1.jar
# spark-submit --conf spark.executor.extraClassPath=sqlite-jdbc-3.30.1.jar --driver-class-path sqlite-jdbc-3.30.1.jar --jars sqlite-jdbc-3.30.1.jar spark.py
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

        df = spark.read.csv(str(file.resolve()), sep=',', schema = schema, ignoreLeadingWhiteSpace = True, 
            ignoreTrailingWhiteSpace = True, header=True)

        output = output.union(df)
    return output

def refactor_type_count(refactoring_miner_df):

    rows = refactoring_miner_df.select("refactoringType").distinct().collect()
    select_statement = []
    for row in rows:
        type = row.refactoringType
        comparison_str = "\'" + row.refactoringType + "\'"
        field_name = type.replace(' ','_') + "_Count"
        sum_str = f"SUM(case refactoringType when {comparison_str} then 1 else 0 end) AS {field_name}"
        select_statement.append(sum_str)
    
    sql_str = f"""
        SELECT 
            projectID,
            commitHash,
            {",".join(select_statement)}
        FROM REFACTORING_MINER GROUP BY projectID,commitHash
    """

    # print(sql_str)

    return spark.sql(sql_str)

if __name__ == "__main__":

    jenkins_data_directory = "./jenkins_data/data"
    sqlite_conn_url = "jdbc:sqlite:/home/hung/MyWorksapce/BachelorThesis/SQLite-Database/technicalDebtDataset.db"

    db = retrieve_sqlite(sqlite_conn_url)
    projects_df = db['PROJECTS']
    refactoring_miner_df = db['REFACTORING_MINER']

    refactoring_miner_df = refactoring_miner_df.filter("refactoringType IS NOT NULL")
    refactoring_miner_df.persist()
    refactoring_miner_df.createOrReplaceTempView("REFACTORING_MINER")

    refactor_count_df = refactor_type_count(refactoring_miner_df)
    refactor_count_df.persist()
    refactoring_miner_df.unpersist()

    jenkins_builds_df = get_jenkins_builds_data(jenkins_data_directory)
    

    jenkins_builds_df = jenkins_builds_df.filter("commit_id IS NOT NULL")
    jenkins_builds_df.persist()

    #jenkins_builds_df.repartition(1).write.csv('./jenkins_builds.csv', header=True)

    result = jenkins_builds_df.join(refactor_count_df, jenkins_builds_df.commit_id == refactor_count_df.commitHash, how = 'inner')
    result.cache()
    
    print("RM Count: ", refactor_count_df.count())
    print("Jenkins Count: ", jenkins_builds_df.count())
    print("Result Count: ",result.count())

    spark.stop()