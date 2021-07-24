# pyspark --driver-class-path postgresql-42.2.12.jar
# spark-submit --driver-class-path postgresql-42.2.12.jar spark_project.py

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pathlib import Path
import time, sys

from spark_utils import *
from pyspark.ml.stat import ChiSquareTest
from pyspark.ml.feature import ChiSqSelector
from pyspark.ml.classification import DecisionTreeClassifier,  RandomForestClassifier
from model_3 import prepare_data_ml3

conf = SparkConf().setMaster('local[*]')
sc = SparkContext
spark = SparkSession.builder.config(conf = conf).getOrCreate()

def issue_impact_process(ml_df, columns, project, organization):

    # ChiSquare
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

    top_issue_lines = []
    data_count = ml_df.count()
    # First importance value being 0 => skip
    if top_10_feaures_importance[1] != 0:
        print("\tFirst ChiSquare selected issue's importance is 0")
        top_issue_lines.append([organization, project, "ChiSquareSelectorModel", data_count] + top_10_feaures_importance)

    # Tree-based algorithm's Feature Importances
    dt = DecisionTreeClassifier(featuresCol='features', labelCol='label', maxDepth=3)
    rf = RandomForestClassifier(featuresCol = 'features', labelCol = 'label', numTrees=10)

    for algo, model_name in [(dt, "DecisionTreeModel"),(rf, "RandomForestModel")]:
        
        model = algo.fit(ml_df)

        f_importances = model.featureImportances
        indices = f_importances.indices.tolist()
        values = f_importances.values.tolist()

        if len(values) < 2:
            print(f"\tOnly less or equal to 1 significant issue for model {model_name}. Skipping writing to Database.")
            continue

        value_index_lst = list(zip(values, indices))
        value_index_lst.sort(key = lambda x: x[0], reverse= True)
        
        importance_sorted_features = []
        for value, index in value_index_lst:
            importance_sorted_features.append(columns[index])
            importance_sorted_features.append(value)

        length = len(importance_sorted_features)
        
        if length > 20:
            importance_sorted_features = importance_sorted_features[:20]
        elif length < 20:
            importance_sorted_features = importance_sorted_features + (20 - length)*[None]

        top_issue_lines.append([organization, project, model_name, data_count] + importance_sorted_features)

    if len(top_issue_lines) > 0:
        top_issue_df = spark.createDataFrame(data = top_issue_lines, schema = TOP_ISSUE_SCHEMA)
        top_issue_df.write.jdbc(CONNECTION_STR, 'top_issues', mode='append', properties=CONNECTION_PROPERTIES)

def run(spark_artefacts_dir, org_keys, servers):
    
    sonar_analyses = get_data_from_db(spark, "sonar_analyses", processed= None, org_server_filter_elements= org_keys, all_columns=True)
    sonar_analyses.persist()

    pairs = list(map(lambda x: (x.organization, x.project), sonar_analyses.select("organization","project").distinct().collect()))

    i = 0
    for organization, project in pairs:
        print(f"{i}. {organization} - {project}:")
        i += 1
        project_sonar_analyses = sonar_analyses.drop("organization", "processed", "ingested_at").filter(sonar_analyses.project == project)
        
        project_sonar_issues = get_data_from_db(spark, "sonar_issues", processed=None, custom_filter=[f"project = '{project}'"], org_server_filter_elements=[organization], all_columns=False)
        project_sonar_issues.persist()

        project_jenkins_builds = get_data_from_db(spark, "jenkins_builds", processed=None, org_server_filter_elements=servers, all_columns=False, \
            custom_filter=[
                f"""revision_number IN (
                    SELECT revision FROM sonar_analyses WHERE project = '{project}'
                    )
                """])
        project_jenkins_builds.persist()

        ml_df, columns = prepare_data_ml3(spark, project_jenkins_builds, project_sonar_issues, project_sonar_analyses, spark_artefacts_dir, "incremental")

        if ml_df is None or ml_df.count() == 0:
            print("\tNot enough data.")
            continue
        
        ml_df.persist()
        print(f"\tCount: {ml_df.count()}")
        issue_impact_process(ml_df, columns, project, organization)

if __name__ == "__main__":
    print("Start Spark Processing per Project.")

    spark_artefacts_dir = "./spark_artefacts"
    artefacts_path = Path(spark_artefacts_dir)
    if not artefacts_path.exists():
        print(f"Path to spark_artefacts at '{artefacts_path.absolute()}' does not exists(). Cancelling!")
        sys.exit(1)

    then = time.time()

    batches = get_batches(CONNECTION_OBJECT)
    if not batches:
        print("No batch to process!")
    for (batch_num, org_keys, servers) in batches:
        print(f"Processing batch {batch_num}")
        print(f"\tSonarcloud organization keys:\n\t\t" + "\n\t\t".join(org_keys))
        print(f"\tJenkins servers:\n\t\t" + "\n\t\t".join(servers))
        run(spark_artefacts_dir, org_keys, servers)

    print(f"Time elapsed: {time.time() - then}")