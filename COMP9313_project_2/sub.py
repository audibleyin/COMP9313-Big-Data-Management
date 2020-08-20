from pyspark.ml.feature import Tokenizer, CountVectorizer, StringIndexer
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType
from pyspark.ml import Pipeline

def mapToInt(nb_pre, svm_pre):
    return float(int(str(int(nb_pre)) + str(int(svm_pre)), 2))


def base_features_gen_pipeline(input_descript_col="descript", input_category_col="category",
                               output_feature_col="features", output_label_col="label"):
    tokenizer = Tokenizer(inputCol=input_descript_col, outputCol="words")
    cv = CountVectorizer(inputCol="words", outputCol=output_feature_col)
    indexer = StringIndexer(inputCol=input_category_col, outputCol=output_label_col)

    return Pipeline(stages=[tokenizer, cv, indexer])


def gen_meta_features(training_df, nb_0, nb_1, nb_2, svm_0, svm_1, svm_2):
    groupNum = training_df.select("group").distinct().count()
    flag = True
    for k in range(groupNum):
        filterCondition = training_df["group"] == k
        dtForTrain = training_df.filter(~filterCondition)
        dtForGenFeature = training_df.filter(filterCondition)
        base_pipeline = Pipeline(stages=[nb_0, nb_1, nb_2, svm_0, svm_1, svm_2])
        if flag:
            resultDF = base_pipeline.fit(dtForTrain).transform(dtForGenFeature)
            flag = False
        else:
            resultDF = resultDF.union(base_pipeline.fit(dtForTrain).transform(dtForGenFeature))
    f = udf(mapToInt, DoubleType())
    resultDF = resultDF.withColumn("joint_pred_0", f("nb_pred_0", "svm_pred_0")) \
        .withColumn("joint_pred_1", f("nb_pred_1", "svm_pred_1")) \
        .withColumn("joint_pred_2", f("nb_pred_2", "svm_pred_2"))
    return resultDF


def test_prediction(test_df, base_features_pipeline_model, gen_base_pred_pipeline_model,
                    gen_meta_feature_pipeline_model, meta_classifier):
    rawTestDF = base_features_pipeline_model.transform(test_df)
    rawTeswithSixDF = gen_base_pred_pipeline_model.transform(rawTestDF)
    f = udf(mapToInt, DoubleType())
    rawTeswithNineDF = rawTeswithSixDF.withColumn("joint_pred_0", f("nb_pred_0", "svm_pred_0")) \
        .withColumn("joint_pred_1", f("nb_pred_1", "svm_pred_1")) \
        .withColumn("joint_pred_2", f("nb_pred_2", "svm_pred_2"))
    featuresDF = gen_meta_feature_pipeline_model.transform(rawTeswithNineDF)
    return meta_classifier.transform(featuresDF).select("id", "label", "final_prediction")
