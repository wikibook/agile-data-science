# Geopy는 위도와 경도로 이루어진 위치 데이터 쌍 사이의 거리를 가져옴 
import geopy

base_path = "."

#
# 우리의 훈련 데이터를 적재하고 공항 수를 셈
#
from pyspark.sql.types import StringType, IntegerType, FloatType, DoubleType, DateType, TimestampType
from pyspark.sql.types import StructType, StructField
from pyspark.sql.functions import udf

schema = StructType([
  StructField("ArrDelay", DoubleType(), True),  # "ArrDelay":5.0
  StructField("CRSArrTime", TimestampType(), True),  # "CRSArrTime":"2015-12-31T03:20:00.000-08:00"
  StructField("CRSDepTime", TimestampType(), True),  # "CRSDepTime":"2015-12-31T03:05:00.000-08:00"
  StructField("Carrier", StringType(), True),  # "Carrier":"WN"
  StructField("DayOfMonth", IntegerType(), True),  # "DayOfMonth":31
  StructField("DayOfWeek", IntegerType(), True),  # "DayOfWeek":4
  StructField("DayOfYear", IntegerType(), True),  # "DayOfYear":365
  StructField("DepDelay", DoubleType(), True),  # "DepDelay":14.0
  StructField("Dest", StringType(), True),  # "Dest":"SAN"
  StructField("Distance", DoubleType(), True),  # "Distance":368.0
  StructField("FlightDate", DateType(), True),  # "FlightDate":"2015-12-30T16:00:00.000-08:00"
  StructField("FlightNum", StringType(), True),  # "FlightNum":"6109"
  StructField("Origin", StringType(), True),  # "Origin":"TUS"
])

features = spark.read.json(
  "data/simple_flight_delay_features.json",
  schema=schema
)
features.registerTempTable("features")

# 출발지와 도착지를 하나의 관계로 가져오고 고유의 코드 개수를 셈 
from pyspark.sql.functions import col
origins = features.select(col("Origin").alias("Airport"))
dests = features.select(col("Dest").alias("Airport"))
distinct_airports = origins.union(dests).distinct()
distinct_airports.count() # 322

#
# Openflights 공항 데이터베이스를 적재하고 검사
#
from pyspark.sql.types import StringType, IntegerType, DoubleType
from pyspark.sql.types import StructType, StructField

airport_schema = StructType([
  StructField("AirportID", StringType(), True),
  StructField("Name", StringType(), True),
  StructField("City", StringType(), True),
  StructField("Country", StringType(), True),
  StructField("FAA", StringType(), True),
  StructField("ICAO", StringType(), True),
  StructField("Latitude", DoubleType(), True),
  StructField("Longitude", DoubleType(), True),
  StructField("Altitude", IntegerType(), True),
  StructField("Timezone", StringType(), True),
  StructField("DST", StringType(), True),
  StructField("TimezoneOlson", StringType(), True)
])

airports = spark.read.format('com.databricks.spark.csv')\
  .options(header='false', inferschema='false')\
  .schema(airport_schema)\
  .load("data/airports.csv")

# ATL을 보여줌
airports.filter(airports.FAA == "ATL").show()
airports.count() # 8107

#
# WBAM 마스터 리스트를 통해 기상 관측소를 확인
#

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

# WBAN 기상 관측소 마스터 리스트를 적재
wban_master_list = spark.read.format('com.databricks.spark.csv')\
  .options(header='true', inferschema='false', delimiter='|')\
  .schema(wban_schema)\
  .load('data/wbanmasterlist.psv')
wban_master_list.count() # 7,411

# 얼마나 많은 기상 관측소가 위치 정보를 가지고 있는가? 
wban_with_location = wban_master_list.filter(
  wban_master_list.LOCATION.isNotNull()
)
wban_with_location.select(
  "WBAN_ID",
  "STATION_NAME",
  "STATE_PROVINCE",
  "COUNTY",
  "COUNTRY",
  "CALL_SIGN",
  "LOCATION"
).show(10)
wban_with_location.count() # 1409

#
# 파싱할 수 있는 위치 정보를 가지고 있는 기상 관측소의 위도/경도를 가져옴
#
from pyspark.sql.functions import split
wban_with_lat_lon = wban_with_location.select(
  wban_with_location.WBAN_ID,
  (split(wban_with_location.LOCATION, ',')[0]).cast("float").alias("Latitude"),
  (split(wban_with_location.LOCATION, ',')[1]).cast("float").alias("Longitude")
)
wban_with_lat_lon.show(10)

# 위도와 경도 정보를 가져온 기상 관측소 수를 셈
wban_with_lat_lon = wban_with_lat_lon.filter(
  wban_with_lat_lon.Longitude.isNotNull()
)
wban_with_lat_lon.count() # 391

#
# geocoding을 통해 위치 수를 확장 
#

# US WBAN의 개수를 셈
us_wbans = wban_master_list.filter("COUNTRY == 'US'")
us_wbans.count() # 4,601

# US WBAN의 주소를 구성
us_wban_addresses = us_wbans.selectExpr(
  "WBAN_ID",
  "CONCAT(STATION_NAME, ', ', COALESCE(COUNTY, ''), ', ', STATE_PROVINCE, ', ', COUNTRY) AS Address"
)
us_wban_addresses.show(100, False)

non_null_us_wban_addresses = us_wban_addresses.filter(
  us_wban_addresses.Address.isNotNull()
)
non_null_us_wban_addresses.count() # 4,597

# 하나의 레코드를 지오코딩 해봄
from geopy.geocoders import Nominatim
geolocator = Nominatim()
geolocator.geocode("MARION COUNTY AIRPORT, MARION, SC, US")

# from socket import timeout
# from geopy.exc import GeocoderTimedOut
from pyspark.sql import Row

def get_location(record):
  geolocator = Nominatim()
  
  latitude = None
  longitude = None
  
  try:
    location = geolocator.geocode(record["Address"])
    if location:
      latitude = location.latitude
      longitude = location.longitude
  except:
    pass
  
  lat_lon_record = Row(
    WBAN_ID=record["WBAN_ID"],
    Latitude=latitude,
    Longitude=longitude,
    Address=record["Address"]
  )
  return lat_lon_record

# 주소를 사용해 WBAM을 지오코딩
wbans_geocoded = us_wban_addresses.rdd.map(get_location).toDF()

# 지오코딩 결과를 저장, 계산에 시간이 오래 걸림
geocoded_wban_output_path = "{}/data/wban_address_with_lat_lon.json".format(
  base_path
)
wbans_geocoded\
  .repartition(1)\
  .write\
  .mode("overwrite")\
  .json(geocoded_wban_output_path)

wbans_geocoded = spark.read.json(
  geocoded_wban_output_path
)

# 인코딩에 성공한 WBAN의 개수를 셈
non_null_wbans_geocoded = wbans_geocoded.filter(
  wbans_geocoded.Longitude.isNotNull()
)
non_null_wbans_geocoded.show()
non_null_wbans_geocoded.count() # 1,299

# 원래의 wban_with_lat_lon을 our non_null_wbans_geocoded와 결합
trimmed_geocoded_wbans = non_null_wbans_geocoded.select(
  "WBAN_ID",
  "Latitude",
  "Longitude"
)
comparable_wbans = trimmed_geocoded_wbans\
  .union(wban_with_lat_lon)\
  .distinct()
comparable_wbans.show(10)
comparable_wbans.count() # 1,692

#
# 공항의 크기를 데이터의 크기로 제한 
#
feature_airports = airports.join(
  distinct_airports,
  on=airports.FAA == distinct_airports.Airport
)
feature_airports.count() # 322

# 필수 열만 유지
trimmed_feature_airports = feature_airports.select(
  "Airport",
  "Latitude",
  "Longitude"
)
trimmed_feature_airports.show(10)

#
# 기상 관측소와 공항을 연결하고 그 사이의 거리를 계산
#

# 모든 쌍에 카티시안 조인을 수행
airport_wban_combinations = trimmed_feature_airports.rdd.cartesian(comparable_wbans.rdd)
airport_wban_combinations.count() # 544,824

# 공항과 기상 관측소 사이의 최단 거리 계산 
from geopy.distance import vincenty
def airport_station_distance(record):
  
  airport = record[0]
  station = record[1]
  
  airport_lat_lon = (airport["Latitude"], airport["Longitude"])
  station_lat_lon = (station["Latitude"], station["Longitude"])
  
  # 기본 값으로 설정된 거리는 매우 크다.
  distance = 24902 # 적도 둘레에 해당
  try:
    distance = round(vincenty(airport_lat_lon, station_lat_lon).miles)
  except:
    pass

  distance_record = Row(
    WBAN_ID=station["WBAN_ID"],
    StationLatitude=station["Latitude"],
    StationLongitude=station["Longitude"],
    Airport=airport["Airport"],
    AirportLatitude=airport["Latitude"],
    AirportLongitude=airport["Longitude"],
    Distance=distance,
  )
  return distance_record

# 우리가 비교할 쌍에 계산 로직을 적용
distances = airport_wban_combinations.map(
  airport_station_distance
)
airport_station_distances = distances.toDF()

# 거리 기준으로 정렬하고 거리가 가까운 쌍을 보여줌
airport_station_distances.orderBy("Distance").show()

# 거리가 가까운 쌍을 저장하고 적재, 이를 계산하는 비용이 많이 듦
distances_output_path = "{}/data/airport_station_distances.json".format(
  base_path
)

airport_station_distances\
  .write\
  .mode("overwrite")\
  .json(distances_output_path)

airport_station_distances = spark.read.json(
  distances_output_path
)

#
# 각 공항에 가장 가까운 기상 관측소를 할당
#
grouped_distances = airport_station_distances.rdd.groupBy(
  lambda x: x["Airport"]
)

def get_min_distance_station(record):
  airport = record[0]
  distances = record[1]
  
  closest_station = min(distances, key=lambda x: x["Distance"])
  return Row(
    Airport=closest_station["Airport"],
    WBAN_ID=closest_station["WBAN_ID"],
    Distance=closest_station["Distance"]
  )
  
cs = grouped_distances.map(get_min_distance_station)
closest_stations = cs.toDF()

# 마지막으로 이 기상 관측소와 공항 쌍을 저장
closest_stations_path = "{}/data/airport_station_pairs.json".format(
  base_path
)
closest_stations\
  .repartition(1)\
  .write\
  .mode("overwrite")\
  .json(closest_stations_path)

# 최선의 결과와 최악의 결과를 보여줌
closest_stations.orderBy("Distance").show(10)
closest_stations.orderBy("Distance", ascending=False).show(10)

# 거리에 대한 히스토그램을 확인
distance_histogram = closest_stations.select("Distance")\
  .rdd\
  .flatMap(lambda x: x)\
  .histogram(
    [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 200, 500, 1100]
  )
distance_histogram
