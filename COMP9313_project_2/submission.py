from pyspark.ml import Pipeline
from pyspark.ml.feature import Tokenizer, CountVectorizer, StringIndexer
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType

def ToFloat(nb_pre, svm_pre):
    if nb_pre==0 and svm_pre == 0:  return 0.0
    if nb_pre==0 and svm_pre == 1:  return 1.0
    if nb_pre == 1 and svm_pre == 0:  return 2.0
    if nb_pre == 1 and svm_pre == 1:  return 3.0

def union(df):
    for k in range(3):
        fil = udf(ToFloat, DoubleType())
        df = df.withColumn( ("joint_pred_%i" %k ),fil( ("nb_pred_%i" %k), ("svm_pred_%i" %k))).cache()
    return df

def base_features_gen_pipeline(input_descript_col="descript", input_category_col="category", output_feature_col="features", output_label_col="label"):
    tok = Tokenizer(inputCol=input_descript_col, outputCol="items")
    cv = CountVectorizer(inputCol="items", outputCol=output_feature_col)
    i = StringIndexer(inputCol=input_category_col, outputCol=output_label_col)
    return Pipeline(stages=[tok, cv, i])

def gen_meta_features(training_df, nb_0, nb_1, nb_2, svm_0, svm_1, svm_2):
    groupNum = training_df.select("group").distinct().count()
    base_pipeline = Pipeline(stages=[nb_0, nb_1, nb_2, svm_0, svm_1, svm_2])
    i = 0
    for n in range(groupNum):

        train = training_df.filter(training_df["group"] != n).cache()
        feature = training_df.filter(training_df["group"] == n).cache()
        if i == 0:
            df_result = base_pipeline.fit(train)
            df_result = df_result.transform(feature).cache()
            i += 1
        else:
            res = base_pipeline.fit(train).transform(feature).cache()
            df_result = df_result.union(res).cache()

    train_res = union(df_result)
    return train_res

def test_prediction(test_df, base_features_pipeline_model, gen_base_pred_pipeline_model, gen_meta_feature_pipeline_model, meta_classifier):
    test = base_features_pipeline_model.transform(test_df)
    df_test = gen_base_pred_pipeline_model.transform(test).cache()
    df_union = union(df_test)
    df_features = gen_meta_feature_pipeline_model.transform(df_union).cache()
    test_meta = meta_classifier.transform(df_features)
    test_filter = test_meta.select("id","label","final_prediction")
    return test_filter



# 0.7483312619309965
# running time: 115.97996425628662

# 0.7483312619309965
# running time: 114.17414140701294