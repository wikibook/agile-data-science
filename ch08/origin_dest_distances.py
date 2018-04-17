import sys, os, re
import json

# 정시 운항 실적 파케이 파일 적재
on_time_dataframe = spark.read.parquet('data/on_time_performance.parquet')
on_time_dataframe.registerTempTable("on_time_performance")

origin_dest_distances = spark.sql("""
  SELECT Origin, Dest, AVG(Distance) AS Distance
  FROM on_time_performance
  GROUP BY Origin, Dest
  ORDER BY Distance
  """)
origin_dest_distances.repartition(1).write.mode("overwrite").json("data/origin_dest_distances.json")
os.system("cp data/origin_dest_distances.json/part* data/origin_dest_distances.jsonl")
