# 비행 지연 레코드를 포함한 파케이 파일 적재
on_time_dataframe = spark.read.parquet('data/on_time_performance.parquet')

# Spark SQL을 위한 데이터 등록
on_time_dataframe.registerTempTable("on_time_performance")

# 컬럼 확인
on_time_dataframe.columns

# 일부 데이터 확인
on_time_dataframe\
  .select("FlightDate", "TailNum", "Origin", "Dest", "Carrier", "DepDelay", "ArrDelay")\
  .show()

# 필드를 트리밍하고 결과 유지
trimmed_on_time = on_time_dataframe\
  .select(
    "FlightDate",
    "TailNum",
    "Origin",
    "Dest",
    "Carrier",
    "DepDelay",
    "ArrDelay"
  )

# 데이터의 0.01%를 샘플링해서 보여줌
trimmed_on_time.sample(False, 0.0001).show()
