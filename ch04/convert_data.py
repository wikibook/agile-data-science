# 한 줄로 헤더 파싱과 타입 추론으로 CSV 적재 
on_time_dataframe = spark.read.format('com.databricks.spark.csv')\
  .options(
    header='true',
    treatEmptyValuesAsNulls='true',
  )\
  .load('data/On_Time_On_Time_Performance_2015.csv.bz2')
on_time_dataframe.registerTempTable("on_time_performance")

trimmed_cast_performance = spark.sql("""
SELECT
  Year, Quarter, Month, DayofMonth, DayOfWeek, FlightDate,
  Carrier, TailNum, FlightNum,
  Origin, OriginCityName, OriginState,
  Dest, DestCityName, DestState,
  DepTime, cast(DepDelay as float), cast(DepDelayMinutes as int),
  cast(TaxiOut as float), cast(TaxiIn as float),
  WheelsOff, WheelsOn,
  ArrTime, cast(ArrDelay as float), cast(ArrDelayMinutes as float),
  cast(Cancelled as int), cast(Diverted as int),
  cast(ActualElapsedTime as float), cast(AirTime as float),
  cast(Flights as int), cast(Distance as float),
  cast(CarrierDelay as float), cast(WeatherDelay as float), cast(NASDelay as float),
  cast(SecurityDelay as float), cast(LateAircraftDelay as float),
  CRSDepTime, CRSArrTime
FROM
  on_time_performance
""")

# 우리의 새로운 정돈된 테이블로 on_time_performance 테이블을 대체하고 그 내용을 보여줌
trimmed_cast_performance.registerTempTable("on_time_performance")
trimmed_cast_performance.show()

# 숫자 컬럼을 합산할 수 있는지 검증
spark.sql("""SELECT
  SUM(WeatherDelay), SUM(CarrierDelay), SUM(NASDelay),
  SUM(SecurityDelay), SUM(LateAircraftDelay)
FROM on_time_performance
""").show()

# 레코드를 gzip으로 압축된 json lines로 저장
trimmed_cast_performance.toJSON()\
  .saveAsTextFile(
    'data/on_time_performance.jsonl.gz',
    'org.apache.hadoop.io.compress.GzipCodec'
  )

# 파일시스템의 레코드 확인
# gunzip -c data/on_time_performance.jsonl.gz/part-00000.gz | head

# Parquet를 사용해서 레코드 저장
trimmed_cast_performance.write.mode("overwrite").parquet("data/on_time_performance.parquet")

# JSON 레코드를 다시 적재
on_time_dataframe = spark.read.json('data/on_time_performance.jsonl.gz')
on_time_dataframe.show()

# 파케이 파일을 다시 적재
on_time_dataframe = spark.read.parquet('data/on_time_performance.parquet')
on_time_dataframe.show()
