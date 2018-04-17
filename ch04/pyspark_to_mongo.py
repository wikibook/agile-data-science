import pymongo
import pymongo_spark
# 중요: pymongo_spark 활성화
pymongo_spark.activate()

on_time_dataframe = spark.read.parquet('data/on_time_performance.parquet')

# https://jira.mongodb.org/browse/HADOOP-276을 피하기 위해 행을 dict로 변환해야 함을 기억할 것
as_dict = on_time_dataframe.rdd.map(lambda row: row.asDict())
as_dict.saveToMongoDB('mongodb://localhost:27017/agile_data_science.on_time_performance')
