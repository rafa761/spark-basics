from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

data_path = 'C:\\Projetos\\spark-basics\\data'

json_df2_path = data_path + '\\example_5.json'
# df1 = spark.read.format("json").load(json_df1_path)

df2 = spark.read.json(json_df2_path, multiLine=True)
df2.count()
df2.show()

df2.columns

df2_sample = df2.sample(False, fraction=0.1)

df2_sample.show()

df2_sorted = df2.sort("color")
df2_sorted.show()
