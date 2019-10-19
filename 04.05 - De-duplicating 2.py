from pyspark.sql import Row

df_dup = sc.parallelize([Row(server_name="101 Server", cpu_utilization=85, session_count=80), \
                         Row(server_name="101 Server", cpu_utilization=80, session_count=90), \
                         Row(server_name="102 Server", cpu_utilization=85, session_count=80), \
                         Row(server_name="102 Server", cpu_utilization=85, session_count=80)]).toDF()

df_dup.show()

df_dup.drop_duplicates().show()

df_dup.drop_duplicates(["server_name"]).show()
