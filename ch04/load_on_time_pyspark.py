# 한 줄로 헤더 파싱과 타입 추론해서 CSV 적재 
# 이 작업을 위해서는 'pyspark --packages com.databricks:spark-csv_2.10:1.4.0'을 사용해야 함
on_time_dataframe = spark.read.format('com.databricks.spark.csv')\
  .options(header='true', inferschema='true')\
  .load('data/On_Time_On_Time_Performance_2015.csv.bz2')

# 데이터 확인 - 너무 넓어서 보기 어려움
on_time_dataframe.show()

# 데이터를 쿼리하기 위해 SQL 사용 - 어느 공항 사이에 가장 많은 비행을 포함하고 있을까? 
on_time_dataframe.registerTempTable("on_time_dataframe")
airport_pair_totals = spark.sql("""SELECT
  Origin, Dest, COUNT(*) AS total
  FROM on_time_dataframe
  GROUP BY Origin, Dest
  ORDER BY total DESC"""
)

# 데이터플로우 사용
airport_pair_totals.limit(10).show()