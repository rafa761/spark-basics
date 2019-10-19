from pyspark.sql import SparkSession
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans

spark = SparkSession.builder.getOrCreate()

data_path = 'C:\\Projetos\\spark-basics\\data'

json_df1_path = data_path + '\\example_8.json'

df = spark.read.json(json_df1_path, multiLine=True)

df.show()

vectorAssembler = VectorAssembler(inputCols=["diameter", "value"], outputCol="features")

vcluster_df = vectorAssembler.transform(df)

vcluster_df.show()


kmeans = KMeans().setK(2)
kmeans = kmeans.setSeed(1)

kmodel = kmeans.fit(vcluster_df)

kmodel.clusterCenters()
