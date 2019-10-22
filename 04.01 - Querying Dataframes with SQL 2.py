import os
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

data_path = os.getcwd() + '\\data'

json_df1_path = data_path + '\\example_6.json'

df1 = spark.read.json(json_df1_path, multiLine=True)

df1.show(10)

df1.createOrReplaceTempView("colors")

df_sql = spark.sql("select * from colors limit 10")

df_sql.show()

df_sql_2 = spark.sql("select color as COR, value as VAL from colors")
df_sql_2.show()
