from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

data_path = 'C:\\Projetos\\spark-basics\\data'

json_df1_path = data_path + '\\example_5.json'
# df1 = spark.read.format("json").load(json_df1_path)

df1 = spark.read.json(json_df1_path, multiLine=True)
df1.count()
df1.show()

df1.columns

df1.filter(df1["color"]=="blue").show()

df1.filter(df1["color"]=="blue").count()
