# pyspark --driver-class-path postgresql-42.2.12.jar
# spark-submit --driver-class-path postgresql-42.2.12.jar spark.py

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pathlib import Path
import time, argparse

from model_1 import apply_ml1
from model_2 import apply_ml2
from model_3 import apply_ml3

conf = SparkConf().setMaster('local[*]')
sc = SparkContext
spark = SparkSession.builder.config(conf = conf).getOrCreate()

def run(spark_artefacts_dir, run_mode):

    # Check for resources that enable incremental run
    if run_mode == "incremental":
        # Check for Spark models, pipelines...
        for i in ['1','2','3']:
            for suffix in ["", "_top_10"]:
                for obj in [f"pipeline_{i}",f"LogisticRegressionModel_{i}{suffix}",f"DecisionTreeModel_{i}{suffix}",f"RandomForestModel_{i}{suffix}", 
                        f"ChiSquareSelectorModel_{i}", "label_indexer_3"]:

                    obj_path = Path(spark_artefacts_dir).joinpath(obj)
                    if not obj_path.exists():
                        print(f"{obj} does not exist in spark_artefacts. Rerun with run_mode = first")
                        run(spark_artefacts_dir, "first")
                        return

    # APPLY MACHINE LEARNING
    apply_ml1(spark, spark_artefacts_dir, run_mode)
    apply_ml2(spark, spark_artefacts_dir, run_mode)
    apply_ml3(spark, spark_artefacts_dir, run_mode)

if __name__ == "__main__":

    ap = argparse.ArgumentParser(description="Central Spark processing")
    ap.add_argument("-a","--artefacts", default='./spark_artefacts' , help="Path to Spark artefacts folder")
    ap.add_argument("-m","--mode", default='incremental' , help="Operation mode", choices=["incremental", "first"])

    args = vars(ap.parse_args())
    spark_artefacts_dir = args['artefacts']
    mode = args['mode']

    then = time.time()
    print(f"Start Spark processing - mode {mode.upper()}")
    run(spark_artefacts_dir, mode)
    spark.stop()
    print(f"Time elapsed: {time.time() - then}")

