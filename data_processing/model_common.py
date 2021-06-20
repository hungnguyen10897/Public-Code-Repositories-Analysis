from pathlib import Path
from pyspark.ml import PipelineModel
from pyspark.ml.feature import ChiSqSelector, ChiSqSelectorModel
from pyspark.sql.types import Row
from pyspark.ml.stat import ChiSquareTest
from pyspark.ml.classification import LogisticRegression, LogisticRegressionModel, DecisionTreeClassifier, DecisionTreeClassificationModel, RandomForestClassifier, RandomForestClassificationModel
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, BinaryClassificationEvaluator
from pyspark.ml.linalg import DenseVector

from spark_utils import *

# For ML model 1 and 2
def pipeline_process(df, get_pipeline_callback, spark_artefacts_dir, run_mode, i):

    pipeline_path = Path(spark_artefacts_dir).joinpath(f"pipeline_{i}")
    if run_mode == "first":
        pipeline = get_pipeline_callback()
        pipeline_model = pipeline.fit(df)
        pipeline_model.write().overwrite().save(str(pipeline_path.absolute()))

    elif run_mode == "incremental":
        pipeline_model = PipelineModel.load(str(pipeline_path.absolute()))

    return pipeline_model.transform(df).select('features','label'), pipeline_model

def get_categorical_columns(ohe_category_sizes):

    cols = []
    for column, size in zip(ML1_CATEGORICAL_COLUMNS, ohe_category_sizes):
        for i in range(size):
            cols.append(f"{column}_index_{str(i)}")

    return cols

def feature_selector_process(spark, ml_df, spark_artefacts_dir, run_mode, i, feature_cols):

    # APPLY CHI-SQUARE SELECTOR
    name = f"ChiSquareSelectorModel_{i}"
    selector_model_path = Path(spark_artefacts_dir).joinpath(name)

    if run_mode == 'first':

        # ChiSq Test to obtain ChiSquare values (higher -> more dependence between feature and lable -> better)
        r = ChiSquareTest.test(ml_df, "features", "label")
        pValues = r.select("pvalues").collect()[0][0].tolist()
        stats = r.select("statistics").collect()[0][0].tolist()
        dof = r.select("degreesOfFreedom").collect()[0][0]

        # ChiSq Selector
        selector =ChiSqSelector(numTopFeatures= 10, featuresCol="features", outputCol="selected_features", labelCol="label")
        selector_model = selector.fit(ml_df)       
        selector_model.write().overwrite().save(str(selector_model_path.absolute()))

        top_10_feaures_importance = []
        top_10_features = []
        for j in selector_model.selectedFeatures:
            top_10_feaures_importance.append(feature_cols[j])
            top_10_features.append(feature_cols[j])
            top_10_feaures_importance.append(stats[j])

        model_info = [name, ml_df.count(), None, None, None, None, None, None, None] + top_10_feaures_importance
        model_info_df = spark.createDataFrame(data = [model_info], schema = MODEL_INFO_SCHEMA)
        model_info_df.write.jdbc(CONNECTION_STR, 'model_info', mode='append', properties=CONNECTION_PROPERTIES)

    elif run_mode == 'incremental':
        selector_model = ChiSqSelectorModel.load(str(selector_model_path.absolute()))
        top_10_features = []
        for j in selector_model.selectedFeatures:
            top_10_features.append(feature_cols[j])

    ml_df_10 = selector_model.transform(ml_df)
    ml_df_10 = ml_df_10.drop("features")

    #Solve a problem with ChiSqSelector and Tree-based algorithm
    ml_rdd_10 = ml_df_10.rdd.map(lambda row: Row(label = row[0], features =DenseVector(row[1].toArray())))
    ml_df_10 = spark.createDataFrame(ml_rdd_10)

    return ml_df_10, top_10_features

def train_predict(spark, ml_df, spark_artefacts_dir, run_mode, i, columns, top_10 = False):

    suffix = "_top_10" if top_10 else ""
    lr_model_name = f"LogisticRegressionModel_{i}" + suffix
    dt_model_name = f"DecisionTreeModel_{i}" + suffix
    rf_model_name = f"RandomForestModel_{i}" + suffix

    if run_mode == "first":

        train,test = ml_df.randomSplit([0.7, 0.3])
        train.persist()
        test.persist()

        replication_factor = 10
        negative_label_train = train.filter("label = 1.0")
        negative_label_train.persist()
        negative_label_train.collect()

        for i in range(replication_factor):
            train = train.union(negative_label_train)
            train.persist()
            
        train.persist()
        train_count = train.count()
        print("Training Dataset Count: " + str(train_count))
        test_count = test.count()
        print("Test Dataset Count: " + str(test_count))

        lr = LogisticRegression(featuresCol='features', labelCol='label', maxIter=10)
        dt = DecisionTreeClassifier(featuresCol='features', labelCol='label', maxDepth=5)
        rf = RandomForestClassifier(featuresCol = 'features', labelCol = 'label', numTrees=100)

        model_performance_lines = []
        model_info_lines = []
        for algo, model_name in [(lr, lr_model_name), (dt, dt_model_name), (rf,rf_model_name)]:
            print(model_name)
            model = algo.fit(train)
            model_path = Path(spark_artefacts_dir).joinpath(model_name)
            model.write().overwrite().save(str(model_path.absolute()))

            # Tree-based algorithm's Feature Importances:
            if algo in [dt, rf]:

                f_importances = model.featureImportances
                indices = f_importances.indices.tolist()
                values = f_importances.values.tolist()

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

            else:
                importance_sorted_features = 20 * [None]
            
            predictions = model.transform(test)
            predictions.persist()
            predictions.show(5)

            train_predictions = model.transform(train)
            train_predictions.persist()
            train_predictions.show(5)

            measures = []
            train_measures = []

            ma_eval = MulticlassClassificationEvaluator()
            for metricName in ["f1","weightedPrecision","weightedRecall","accuracy"]:
                measure = ma_eval.evaluate(predictions, {ma_eval.metricName: metricName})
                measures.append(measure)
                print(f"\t{metricName}: {measure}")

                train_measure = ma_eval.evaluate(train_predictions, {ma_eval.metricName: metricName})
                train_measures.append(train_measure)
                print(f"\tTrain-{metricName}: {train_measure}")

            bin_eval = BinaryClassificationEvaluator()
            for metricName in ["areaUnderROC" , "areaUnderPR"]:
                measure = bin_eval.evaluate(predictions, {bin_eval.metricName: metricName})
                measures.append(measure)
                print(f"\t{metricName}: {measure}")

                train_measure = bin_eval.evaluate(train_predictions, {bin_eval.metricName: metricName})
                train_measures.append(train_measure)
                print(f"\tTrain-{metricName}: {train_measure}")

            # Predicted negatives
            negs =  predictions.select("label").filter("label = 1.0").count()
            if negs != 0:
                predicted_negative_rate = predictions.select("label").filter("label = 1.0 AND prediction = 1.0").count() / negs
            else:
                predicted_negative_rate = None
            print(f"\tpredicted_negative_rate: {predicted_negative_rate}")
            measures.append(predicted_negative_rate)

            train_negs = train_predictions.select("label").filter("label = 1.0").count()
            if train_negs != 0:
                train_predicted_negative_rate = train_predictions.select("label").filter("label = 1.0 AND prediction = 1.0").count() / train_negs
            else:
                train_predicted_negative_rate = None
            print(f"\ttrain_predicted_negative_rate: {train_predicted_negative_rate}")
            train_measures.append(train_predicted_negative_rate)
            
            model_performance_lines.append([model_name, test_count] + measures)
            model_info_lines.append([model_name, train_count] + train_measures + importance_sorted_features)
            
        model_performance_df = spark.createDataFrame(data= model_performance_lines, schema = MODEL_PERFORMANCE_SCHEMA)
        model_performance_df.write.jdbc(CONNECTION_STR, 'model_performance', mode = 'append', properties= CONNECTION_PROPERTIES)

        model_info_df = spark.createDataFrame(data= model_info_lines, schema = MODEL_INFO_SCHEMA)
        model_info_df.write.jdbc(CONNECTION_STR, 'model_info', mode = 'append', properties= CONNECTION_PROPERTIES)

    elif run_mode == "incremental":
    
        model_performance_lines = []
        for model, name in [(LogisticRegressionModel, lr_model_name), (DecisionTreeClassificationModel, dt_model_name), (RandomForestClassificationModel, rf_model_name)]:
            
            print("\n\n" + name)
            model_path = Path(spark_artefacts_dir).joinpath(name)
            ml_model = model.load(str(model_path.absolute()))

            predictions = ml_model.transform(ml_df)
            predictions.persist()
            predictions.show(5)

            measures = []
            ma_eval = MulticlassClassificationEvaluator()
            for metricName in ["f1","weightedPrecision","weightedRecall","accuracy"]:
                measure = ma_eval.evaluate(predictions, {ma_eval.metricName: metricName})
                measures.append(measure)
                print(f"\t{metricName}: {measure}")

            bin_eval = BinaryClassificationEvaluator()
            for metricName in ["areaUnderROC" , "areaUnderPR"]:
                measure = bin_eval.evaluate(predictions, {bin_eval.metricName: metricName})
                measures.append(measure)
                print(f"\t{metricName}: {measure}")

            # Predicted negatives
            negatives = predictions.select("label").filter("label = 1.0").count()
            if negatives == 0:
                print("\tNo negative labels.")
                predicted_negative_rate = None
            else:
                predicted_negative_rate = predictions.select("label").filter("label = 1.0 AND prediction = 1.0").count() / predictions.select("label").filter("label = 1.0").count()
                print(f"\tpredicted_negative_rate: {predicted_negative_rate}")
            
            measures.append(predicted_negative_rate)

            model_performance_lines.append([name, ml_df.count()] + measures) 

        model_performance_df = spark.createDataFrame(data= model_performance_lines, schema = MODEL_PERFORMANCE_SCHEMA)
        model_performance_df.write.jdbc(CONNECTION_STR, 'model_performance', mode = 'append', properties = CONNECTION_PROPERTIES)
