import os
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

data_path = os.getcwd() + '\\data'

json_df1_path = data_path + '\\example_6.json'

df1 = spark.read.json(json_df1_path, multiLine=True)

df1.show(10)

df1.createOrReplaceTempView("colors")

df_sql = spark.sql("select count(*) \
                      from colors")

df_sql.show()


df_sql = spark.sql("select * \
                      from colors \
                     where color = 'blue'")

df_sql.show()

df_sql = spark.sql("select color, count(value) \
                      from colors \
                     group by color \
                     order by color desc")

df_sql.show()


df_sql = spark.sql("select color, round(avg(value),2) average, round(sum(value),2)  sum_value, \
                           count(*)  count_value, min(value) min, max(value) max \
                      from colors \
                     group by color \
                     order by sum(value) desc ")

df_sql.show()
