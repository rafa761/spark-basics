from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

data_path = 'C:\\Projetos\\spark-basics\\data'

json_df1_path = data_path + '\\example_6.json'

df1 = spark.read.json(json_df1_path, multiLine=True)

df1.show()


# df_na = df1.withColumn('na_col', lit(None).cast(StringType()))
