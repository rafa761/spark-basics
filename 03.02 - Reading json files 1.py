import os
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

data_path = os.getcwd() + '\\data'

json_df1_path = data_path + '\\example_1.json'
# df1 = spark.read.format("json").load(json_df1_path)

df1 = spark.read.json(json_df1_path, multiLine=True)
df1.count()
df1.show()
