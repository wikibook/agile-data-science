# 정시 운항 실적이 포함된 파케이 파일을 적재
on_time_dataframe = spark.read.parquet('data/on_time_performance.parquet')

# 첫 번째 단계는 SQL로 쉽게 표현: 항공사별 고유한 꼬리 번호를 모두 가져옴
on_time_dataframe.registerTempTable("on_time_performance")
carrier_airplane = spark.sql(
  "SELECT DISTINCT Carrier, TailNum FROM on_time_performance"
  )

