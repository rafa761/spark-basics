from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

data_path = 'C:\\Projetos\\spark-basics\\data'

json_df1_path = data_path + '\\example_6.json'

df1 = spark.read.json(json_df1_path, multiLine=True)

df1.show(10)

df1.createOrReplaceTempView("colors")

df_sql = spark.sql("select * from colors where color = 'blue'")
df_sql.show()

df_sql.count()

df_sql = spark.sql("select color, value \
                      from colors \
                     where value <= 1 \
                     order by color")
df_sql.show()
