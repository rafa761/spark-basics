from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

data_path = 'C:\\Projetos\\spark-basics\\data'

json_df1_path = data_path + '\\example_6.json'

df1 = spark.read.json(json_df1_path, multiLine=True)

df1.show(10)

df1.createOrReplaceTempView("colors")


json_df2_path = data_path + '\\example_7.json'

df2 = spark.read.json(json_df2_path, multiLine=True)

df2.show()

df2.createOrReplaceTempView("names")

df_count = spark.sql("select distinct color from colors")
df_count.show()

df_join = spark.sql("select cl.color, nm.name, cl.value\
                       from colors cl, names nm \
                      where nm.color = cl.color")

df_join.show()
