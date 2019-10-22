import os
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

data_path = os.getcwd() + '\\data'

json_df1_path = data_path + '\\example_6.json'

df1 = spark.read.json(json_df1_path, multiLine=True)

df1.write.csv('df1.csv')
# with open(f'{data_path}\\df1.csv', mode='w') as file:
#    file.write(df1.)


df1.write.json('df1.json')
