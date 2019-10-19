from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

data_path = 'C:\\Projetos\\spark-basics\\data'

json_df1_path = data_path + '\\example_6.json'

df1 = spark.read.json(json_df1_path, multiLine=True)
df1.count()
df1.show()

df1.columns

df1_s1 = df1.sample(fraction=0.1, withReplacement=False)
df1_s1.count()

df1_s1.groupBy("color").agg({'value': 'mean'}).show()

df1_s1.groupBy("color").agg({'value': 'mean'}).orderBy("color").show()


df1.groupBy("color").agg({'value': 'mean'}).orderBy("color").show()
