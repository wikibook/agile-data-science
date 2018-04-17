import sys, os, re

# 정시 운항 실적 파케이 파일 적재
on_time_dataframe = spark.read.parquet('data/on_time_performance.parquet')

# 첫 번째 단계는 SQL로 쉽게 표현됨: 각 항공사에 소속된 모든 고유의 꼬리 번호 가져옴
on_time_dataframe.registerTempTable("on_time_performance")
carrier_codes = spark.sql(
  "SELECT DISTINCT Carrier FROM on_time_performance"
  )
carrier_codes.collect()

from pyspark.sql.types import StringType, IntegerType
from pyspark.sql.types import StructType, StructField

schema = StructType([
  StructField("ID", IntegerType(), True),  # "ArrDelay":5.0
  StructField("Name", StringType(), True),  # "CRSArrTime":"2015-12-31T03:20:00.000-08:00"
  StructField("Alias", StringType(), True),  # "CRSDepTime":"2015-12-31T03:05:00.000-08:00"
  StructField("IATA", StringType(), True),  # "Carrier":"WN"
  StructField("ICAO", StringType(), True),  # "DayOfMonth":31
  StructField("CallSign", StringType(), True),  # "DayOfWeek":4
  StructField("Country", StringType(), True),  # "DayOfYear":365
  StructField("Active", StringType(), True),  # "DepDelay":14.0
])

airlines = spark.read.format('com.databricks.spark.csv')\
  .options(header='false', nullValue='\\N')\
  .schema(schema)\
  .load('data/airlines.csv')
airlines.show()

# 델타 항공이 있는지?
airlines.filter(airlines.IATA == 'DL').show()

# C1을 'Name'으로 C3를 'CarrierCode'로 남기고 나머지 필드 제거
airlines.registerTempTable("airlines")
airlines = spark.sql("SELECT Name, IATA AS CarrierCode from airlines")

# 우리가 가진 14개 항공사 코드를 항공사 테이블에 조인해 우리의 항공사 데이터셋을 만듦
our_airlines = carrier_codes.join(airlines, carrier_codes.Carrier == airlines.CarrierCode)
our_airlines = our_airlines.select('Name', 'CarrierCode')
our_airlines.show()

# dataframe을 통해 JSON 객체로 저장, 하나의 json 파일을 가져오기 위해 repartition을 1로 설정
our_airlines.repartition(1).write.mode("overwrite").json("data/our_airlines.json")

os.system("cp data/our_airlines.json/part* data/our_airlines.jsonl")

#wikidata = spark.read.json('data/wikidata-20160404-all.json.bz2')
