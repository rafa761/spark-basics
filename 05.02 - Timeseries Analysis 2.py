from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

data_path = 'C:\\Projetos\\spark-basics\\data'

json_df1_path = data_path + '\\example_8.json'

df1 = spark.read.json(json_df1_path, multiLine=True)

df1.show()


df1.createOrReplaceTempView("colors")

spark.sql('select color, min(diameter) min_diam, max(diameter) max_diam, stddev(diameter) std_diam \
             from colors \
            group by color').show()

spark.sql("select color, ")
