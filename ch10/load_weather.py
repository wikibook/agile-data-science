# 정시 운항 실적 파케이 파일을 적재
on_time_dataframe = spark.read.parquet('data/on_time_performance.parquet')
on_time_dataframe.registerTempTable("on_time_performance")

from pyspark.sql.types import StringType, IntegerType, DoubleType
from pyspark.sql.types import StructType, StructField

wban_schema = StructType([
  StructField("REGION", StringType(), True),
  StructField("WBAN_ID", StringType(), True),
  StructField("STATION_NAME", StringType(), True),
  StructField("STATE_PROVINCE", StringType(), True),
  StructField("COUNTY", StringType(), True),
  StructField("COUNTRY", StringType(), True),
  StructField("EXTENDED_NAME", StringType(), True),
  StructField("CALL_SIGN", StringType(), True),
  StructField("STATION_TYPE", StringType(), True),
  StructField("DATE_ASSIGNED", StringType(), True),
  StructField("BEGIN_DATE", StringType(), True),
  StructField("COMMENTS", StringType(), True),
  StructField("LOCATION", StringType(), True),
  StructField("ELEV_OTHER", StringType(), True),
  StructField("ELEV_GROUND", StringType(), True),
  StructField("ELEV_RUNWAY", StringType(), True),
  StructField("ELEV_BAROMETRIC", StringType(), True),
  StructField("ELEV_STATION", StringType(), True),
  StructField("ELEV_UPPER_AIR", StringType(), True)
])

# WBAN 기상 관측소 마스터 리스트 적재
wban_master_list = spark.read.format('com.databricks.spark.csv')\
  .options(header='true', inferschema='false', delimiter='|')\
  .schema(wban_schema)\
  .load('data/wbanmasterlist.psv')
wban_master_list.show(5)

# 공항만 필터링
airport_wbans = wban_master_list.filter(
  wban_master_list.STATION_NAME.endswith("AIRPORT")
)
airport_wbans.count() # 338

#
#  on-time-performance 테이블의 공항 수와 비교
#

# 출발 공항/도착 공항에서 공항을 가져옴
origin_airports = spark.sql("""
  SELECT Origin AS Airport
  FROM on_time_performance
""")
dest_airports = spark.sql("""
  SELECT Dest AS Airport
  FROM on_time_performance
""")


# 출발/도착 공항을 하나의 관계로 결합(sql로 만듦)
distinct_airports = origin_airports\
  .union(dest_airports)\
  .distinct()

# 공항 코드의 개수를 가져옴
distinct_airports.count()

# 공항 코드의 개수를 가져옴
distinct_airports.count() # 332

# 날씨 레코드 자체를 적재
hourly_weather_records = spark.read.format('com.databricks.spark.csv')\
  .options(header='true', inferschema='true', delimiter=',')\
  .load('data/2015*hourly.txt.gz')
hourly_weather_records.show()

# 한 기간 동안 (아마) 하나의 기상 관측소에 대해 몇 가지 필드를 보여줌 
trimmed_hourly_weather_records = hourly_weather_records.select(
  hourly_weather_records.WBAN,
  hourly_weather_records.Date,
  hourly_weather_records.Time,
  hourly_weather_records.SkyCondition,
  hourly_weather_records.WeatherType,
  hourly_weather_records.DryBulbCelsius,
  hourly_weather_records.Visibility,
  hourly_weather_records.WindSpeed,
  hourly_weather_records.WindDirection,
)
trimmed_hourly_weather_records.show()

#
# 일별 관측 데이터 적재
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

#
# 일별 관측 데이터 적재
#

# 날씨 레코드 자체를 적재
daily_schema = StructType([
  StructField("WBAN", StringType(), True),
  StructField("YearMonthDay", StringType(), True),
  StructField("Tmax", StringType(), True),
  StructField("TmaxFlag", StringType(), True),
  StructField("Tmin", StringType(), True),
  StructField("TminFlag", StringType(), True),
  StructField("Tavg", StringType(), True),
  StructField("TavgFlag", StringType(), True),
  StructField("Depart", StringType(), True),
  StructField("DepartFlag", StringType(), True),
  StructField("DewPoint", StringType(), True),
  StructField("DewPointFlag", StringType(), True),
  StructField("WetBulb", StringType(), True),
  StructField("WetBulbFlag", StringType(), True),
  StructField("Heat", StringType(), True),
  StructField("HeatFlag", StringType(), True),
  StructField("Cool", StringType(), True),
  StructField("CoolFlag", StringType(), True),
  StructField("Sunrise", StringType(), True),
  StructField("SunriseFlag", StringType(), True),
  StructField("Sunset", StringType(), True),
  StructField("SunsetFlag", StringType(), True),
  StructField("CodeSum", StringType(), True),
  StructField("CodeSumFlag", StringType(), True),
  StructField("Depth", StringType(), True),
  StructField("DepthFlag", StringType(), True),
  StructField("Water1", StringType(), True),
  StructField("Water1Flag", StringType(), True),
  StructField("SnowFall", StringType(), True),
  StructField("SnowFallFlag", StringType(), True),
  StructField("PrecipTotal", StringType(), True),
  StructField("PrecipTotalFlag", StringType(), True),
  StructField("StnPressure", StringType(), True),
  StructField("StnPressureFlag", StringType(), True),
  StructField("SeaLevel", StringType(), True),
  StructField("SeaLevelFlag", StringType(), True),
  StructField("ResultSpeed", StringType(), True),
  StructField("ResultSpeedFlag", StringType(), True),
  StructField("ResultDir", StringType(), True),
  StructField("ResultDirFlag", StringType(), True),
  StructField("AvgSpeed", StringType(), True),
  StructField("AvgSpeedFlag", StringType(), True),
  StructField("Max5Speed", StringType(), True),
  StructField("Max5SpeedFlag", StringType(), True),
  StructField("Max5Dir", StringType(), True),
  StructField("Max5DirFlag", StringType(), True),
  StructField("Max2Speed", StringType(), True),
  StructField("Max2SpeedFlag", StringType(), True),
  StructField("Max2Dir", StringType(), True),
  StructField("Max2DirFlag", StringType(), True),
])

daily_weather_records = spark.read.format('com.databricks.spark.csv')\
  .options(header='true', inferschema='false', delimiter=',')\
  .schema(daily_schema)\
  .load('data/2015*daily.txt.gz')
daily_weather_records.show()
