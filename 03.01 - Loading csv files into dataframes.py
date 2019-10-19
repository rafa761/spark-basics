from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

spark

data_path = 'C:\\Projetos\\spark-basics\\data'
file_path = data_path + '\\spark-data.csv'

df1 = spark.read.format("csv").option("header", "true").load(file_path)
df1.head(10)

df1.show()

df1.count()

# _c0	carat	cut	color	clarity	depth	table	price	x	y	z
file_path_no_header = data_path + '\\spark-data-no-header.csv'
df2 = spark.read.format("csv").option("header", "false").option("InferSchema", "true").load(file_path_no_header)
df2.head(10)

df2.count()

df2.show()

# _c0	carat	cut	color	clarity	depth	table	price	x	y	z
df2 = df2.withColumnRenamed("_c0", "index") \
         .withColumnRenamed("_c1", "carat") \
         .withColumnRenamed("_c2", "cut") \
         .withColumnRenamed("_c3", "color") \
         .withColumnRenamed("_c4", "clarity") \
         .withColumnRenamed("_c5", "depth") \
         .withColumnRenamed("_c6", "table") \
         .withColumnRenamed("_c7", "price") \
         .withColumnRenamed("_c8", "x") \
         .withColumnRenamed("_c9", "y") \
         .withColumnRenamed("_c10", "z")

df2.show()
