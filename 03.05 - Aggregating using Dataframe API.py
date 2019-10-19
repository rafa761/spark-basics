from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

data_path = 'C:\\Projetos\\spark-basics\\data'

json_df1_path = data_path + '\\example_6.json'

df1 = spark.read.json(json_df1_path, multiLine=True)
df1.count()
df1.show()

df1.columns

df1.filter(df1["color"]=="red").show()

df1.filter(df1["color"]=="red").count()


df1.groupby("color").count().show()

df1.orderBy("color").show()

df1.groupBy("color").agg({'value': 'mean'}).show()

df1.groupBy("color").agg({'value': 'sum'}).show()
