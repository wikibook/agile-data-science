#
# 성능 개선을 위해 시간 별 날씨 PSV를 파케이 형식으로 변환 
#

from pyspark.sql.types import StringType, IntegerType, DoubleType
from pyspark.sql.types import StructType, StructField

hourly_schema = StructType([
  StructField("WBAN", StringType(), True),
  StructField("Date", StringType(), True),
  StructField("Time", StringType(), True),
  StructField("StationType", StringType(), True),
  StructField("SkyCondition", StringType(), True),
  StructField("SkyConditionFlag", StringType(), True),
  StructField("Visibility", StringType(), True),
  StructField("VisibilityFlag", StringType(), True),
  StructField("WeatherType", StringType(), True),
  StructField("WeatherTypeFlag", StringType(), True),
  StructField("DryBulbFarenheit", StringType(), True),
  StructField("DryBulbFarenheitFlag", StringType(), True),
  StructField("DryBulbCelsius", StringType(), True),
  StructField("DryBulbCelsiusFlag", StringType(), True),
  StructField("WetBulbFarenheit", StringType(), True),
  StructField("WetBulbFarenheitFlag", StringType(), True),
  StructField("WetBulbCelsius", StringType(), True),
  StructField("WetBulbCelsiusFlag", StringType(), True),
  StructField("DewPointFarenheit", StringType(), True),
  StructField("DewPointFarenheitFlag", StringType(), True),
  StructField("DewPointCelsius", StringType(), True),
  StructField("DewPointCelsiusFlag", StringType(), True),
  StructField("RelativeHumidity", StringType(), True),
  StructField("RelativeHumidityFlag", StringType(), True),
  StructField("WindSpeed", StringType(), True),
  StructField("WindSpeedFlag", StringType(), True),
  StructField("WindDirection", StringType(), True),
  StructField("WindDirectionFlag", StringType(), True),
  StructField("ValueForWindCharacter", StringType(), True),
  StructField("ValueForWindCharacterFlag", StringType(), True),
  StructField("StationPressure", StringType(), True),
  StructField("StationPressureFlag", StringType(), True),
  StructField("PressureTendency", StringType(), True),
  StructField("PressureTendencyFlag", StringType(), True),
  StructField("PressureChange", StringType(), True),
  StructField("PressureChangeFlag", StringType(), True),
  StructField("SeaLevelPressure", StringType(), True),
  StructField("SeaLevelPressureFlag", StringType(), True),
  StructField("RecordType", StringType(), True),
  StructField("RecordTypeFlag", StringType(), True),
  StructField("HourlyPrecip", StringType(), True),
  StructField("HourlyPrecipFlag", StringType(), True),
  StructField("Altimeter", StringType(), True),
  StructField("AltimeterFlag", StringType(), True),
])

# 날씨 레코드 자체를 적재
hourly_weather_records = spark.read.format('com.databricks.spark.csv')\
  .options(header='true', inferschema='false', delimiter=',')\
  .schema(hourly_schema)\
  .load('data/2015*hourly.txt.gz')
hourly_weather_records.show()

hourly_columns = ["WBAN", "Date", "Time", "SkyCondition",
                  "Visibility", "WeatherType", "DryBulbCelsius",
                  "WetBulbCelsius", "DewPointCelsius",
                  "RelativeHumidity", "WindSpeed", "WindDirection",
                  "ValueForWindCharacter", "StationPressure",
                  "SeaLevelPressure", "HourlyPrecip", "Altimeter"]

trimmed_hourly_records = hourly_weather_records.select(hourly_columns)
trimmed_hourly_records.show()

#
# DataFrame udf를 사용해 ISO8601 형식의 ISODate 컬럼을 추가
#
from pyspark.sql.functions import udf

# 기상 관측소 날짜를 ISO 날짜로 변환
def station_date_to_iso(station_date):
  str_date = str(station_date)
  year = str_date[0:4]
  month = str_date[4:6]
  day = str_date[6:8]
  return "{}-{}-{}".format(year, month, day)

extract_date_udf = udf(station_date_to_iso, StringType())
hourly_weather_with_iso_date = trimmed_hourly_records.withColumn(
  "ISODate",
  extract_date_udf(trimmed_hourly_records.Date)
)

# 기상 관측소 시간을 ISO 시간으로 변환 
def station_time_to_iso(station_time):
  hour = station_time[0:2]
  minute = station_time[2:4]
  iso_time = "{hour}:{minute}:00".format(
    hour=hour,
    minute=minute
  )
  return iso_time

extract_time_udf = udf(station_time_to_iso, StringType())
hourly_weather_with_iso_time = hourly_weather_with_iso_date.withColumn(
  "ISOTime",
  extract_time_udf(trimmed_hourly_records.Time)
)

from pyspark.sql.functions import concat, lit
hourly_weather_with_iso_datetime = hourly_weather_with_iso_time.withColumn(
  "Datetime",
  concat(
    hourly_weather_with_iso_time.ISODate,
    lit("T"),
    hourly_weather_with_iso_time.ISOTime
  )
)

#
# 최종 레코드 트리밍, 원래 날짜/시간 필드를 삭제하고 저장
#
from pyspark.sql.functions import col
final_hourly_columns = ["WBAN", col("ISODate").alias("Date"), "Datetime", "SkyCondition",
                        "Visibility", "WeatherType", "DryBulbCelsius",
                        "WetBulbCelsius", "DewPointCelsius",
                        "RelativeHumidity", "WindSpeed", "WindDirection",
                        "ValueForWindCharacter", "StationPressure",
                        "SeaLevelPressure", "HourlyPrecip", "Altimeter"]
final_trimmed_hourly_records = hourly_weather_with_iso_datetime.select(
  final_hourly_columns
)
final_trimmed_hourly_records.show(5)

# 성능 개선을 위해 파케이를 사용해서 정리된 레코드 저장 
final_trimmed_hourly_records\
  .write\
  .mode("overwrite")\
  .parquet("data/2015_hourly_observations.parquet")
