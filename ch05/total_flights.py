# 파케이 파일 적재
on_time_dataframe = spark.read.parquet('data/on_time_performance.parquet')

# SQL을 사용해서 2015년 월 별 총 비행 수를 검토
on_time_dataframe.registerTempTable("on_time_dataframe")
total_flights_by_month = spark.sql(
  """SELECT Month, Year, COUNT(*) AS total_flights
  FROM on_time_dataframe
  GROUP BY Year, Month
  ORDER BY Year, Month"""
)

# map/asDict를 사용하면 행을 좀 더 예쁘게 출력할 수 있음. 선택사항임. 
flights_chart_data = total_flights_by_month.rdd.map(lambda row: row.asDict())
flights_chart_data.collect()

# 차트를 몽고DB에 저장
import pymongo_spark
pymongo_spark.activate()
flights_chart_data.saveToMongoDB(
  'mongodb://localhost:27017/agile_data_science.flights_by_month'
)

