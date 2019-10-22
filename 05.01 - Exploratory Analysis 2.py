import os
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

data_path = os.getcwd() + '\\data'

json_df1_path = data_path + '\\example_8.json'

df1 = spark.read.json(json_df1_path, multiLine=True)

df1.show()


df1.createOrReplaceTempView("colors")

df1.count()

df1.describe().show()

# Verify the correlation
df1.stat.corr('diameter', 'value')

# Verify the most frequent item
df1.stat.freqItems(('diameter', 'value')).show()

df_sample = df1.sample(fraction=0.05, withReplacement=False)
df_sample.show()
df_sample.count()


spark.sql("select min(diameter) min_diam, max(diameter) max_diam, stddev(diameter) std_diamater \
             from colors").show()


spark.sql("select color, min(diameter) min_diam, max(diameter) max_diam, stddev(diameter) std_diamater \
             from colors \
            group by color").show()

spark.sql("select color, floor(diameter*100/10) bucket from colors").show()


spark.sql("select count(*), floor(diameter*100/10) bucket \
             from colors \
            where group by bucket \
            order by bucket").show()
            
