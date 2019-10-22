import os
from pyspark.sql import SparkSession
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler

spark = SparkSession.builder.getOrCreate()

data_path = os.getcwd() + '\\data'

json_df1_path = data_path + '\\example_8.json'

df = spark.read.json(json_df1_path, multiLine=True)

df.show()

vectorAssembler = VectorAssembler(inputCols=["diameter"], outputCol="features")

df_vector = vectorAssembler.transform(df)

df_vector.show()

lr = LinearRegression(featuresCol="features", labelCol="diameter")

lrModel = lr.fit(df_vector)

lrModel.coefficients

lrModel.intercept

lrModel.summary.rootMeanSquaredError
