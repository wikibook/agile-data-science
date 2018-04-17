# 시간별 관측 결과를 적재
hourly_weather_records = spark.read.parquet("data/2015_hourly_observations.parquet")
hourly_weather_records.show()

# 관측 결과를 기상 관측소(WBAN)와 날짜 단위로 그룹화
# 그런 다음 해당 날짜의 시간 별 관측 결과의 목록을 수집
from frozendict import frozendict
records_per_station_per_day = hourly_weather_records\
  .repartition(3)\
  .rdd\
  .map(
    lambda row: (
      frozendict({
        'WBAN': row.WBAN, # compound key
        'Date': row.Date,
      }),
      [
        { # omit WBAN and Date
          'WBAN': row.WBAN,
          'Datetime': row.Datetime,
          'SkyCondition': row.SkyCondition,
          'Visibility': row.Visibility,
          'WeatherType': row.WeatherType,
          'DryBulbCelsius': row.DryBulbCelsius,
          'WetBulbCelsius': row.WetBulbCelsius,
          'DewPointCelsius': row.DewPointCelsius,
          'RelativeHumidity': row.RelativeHumidity,
          'WindSpeed': row.WindSpeed,
          'WindDirection': row.WindDirection,
          'ValueForWindCharacter': row.ValueForWindCharacter,
          'StationPressure': row.StationPressure,
          'SeaLevelPressure': row.SeaLevelPressure,
          'HourlyPrecip': row.HourlyPrecip,
          'Altimeter': row.Altimeter,
        }
      ]
    )
  )\
  .reduceByKey(lambda a, b: a + b)\
  .map(lambda tuple:
    { # 복합 키 - WBAN과 날짜
      'WBAN': tuple[0]['WBAN'],
      'Date': tuple[0]['Date'],
      # 관측 결과를 날자/시간으로 정렬
      'Observations': sorted(
        tuple[1], key=lambda x: x['Datetime']
      )
    }
  )

# 계산에 비용이 많이 들기 때문에 결과를 저장하고 적재함
import json
per_day_output = "data/observations_per_station_per_day.json"
records_per_station_per_day\
  .map(json.dumps)\
  .saveAsTextFile(per_day_output)
records_per_station_per_day = sc\
  .textFile(per_day_output)\
  .map(json.loads)
records_per_station_per_day.first()

# 우리 관측 레코드를 WBAN 조인 키를 가진 튜플에 래핑(wrapping)
wrapped_daily_records = records_per_station_per_day.map(
  lambda record: (
    record['WBAN'],
    record
  )
)

# WBAN 기상 관측소 마스터 리스트를 적재
wban_master_list = spark.read.format('com.databricks.spark.csv')\
  .options(header='true', inferschema='true', delimiter='|')\
  .load('data/wbanmasterlist.psv')
wban_master_list.show(5)

# 두 레코드 집합의 WBAN ID가 어떻게 생겼을까?
wbans_one = wban_master_list.select('WBAN_ID').distinct().sort('WBAN_ID')
wbans_one.show(6, False)
wbans_two = hourly_weather_records.select('WBAN').distinct().sort('WBAN')
wbans_two.show(6, False)

# WBAN 마스터 리스트를 우리가 관심 있는 것으로 트리밍
trimmed_wban_master_list = wban_master_list.select(
  'WBAN_ID',
  'STATION_NAME',
  'STATE_PROVINCE',
  'COUNTRY',
  'EXTENDED_NAME',
  'CALL_SIGN',
  'STATION_TYPE',
  'LOCATION',
  'ELEV_GROUND',
)
trimmed_wban_master_list.show()

# 이제 이를 (join_key, value) 튜플 형식으로 만듦
joinable_wban_master_list = trimmed_wban_master_list.rdd.map(
  lambda record:
    (
      record.WBAN_ID,
      {
        'WBAN_ID': record.WBAN_ID,
        'STATION_NAME': record.STATION_NAME,
        'STATE_PROVINCE': record.STATE_PROVINCE,
        'COUNTRY': record.COUNTRY,
        'EXTENDED_NAME': record.EXTENDED_NAME,
        'CALL_SIGN': record.CALL_SIGN,
        'STATION_TYPE': record.STATION_TYPE,
        'LOCATION': record.LOCATION,
        'ELEV_GROUND': record.ELEV_GROUND,
      }
    )
)
joinable_wban_master_list.take(1)

# 이제 조인할 준비가 됐다.
profile_with_observations = wrapped_daily_records.join(joinable_wban_master_list)

# 성능을 위해 조인 결과를 저장하고 적재 
joined_output = "data/joined_profile_observations.json"
profile_with_observations\
  .map(json.dumps)\
  .saveAsTextFile(joined_output)
profile_with_observations = sc.textFile(joined_output)\
  .map(json.loads)

profile_with_observations.take(1)

# 이제 이 거대한 결과물은 우리가 몽고DB에 넣고 싶은 것으로 변환
def cleanup_joined_records(record):
  wban = record[0]
  join_record = record[1]
  observations = join_record[0]
  profile = join_record[1]
  return {
    'Profile': profile,
    'Date': observations['Date'],
    'WBAN': observations['WBAN'],
    'Observations': observations['Observations'],
  }

# pyspark.RDD.foreach()는 RDD의 모든 레코드에 대해 함수를 실행
cleaned_station_observations = profile_with_observations.map(cleanup_joined_records)
one_record = cleaned_station_observations.take(5)[2]

# 우리가 실제로 확인할 수 있는 방식으로 출력 
import json
print(json.dumps(one_record, indent=2))

# 기상 관측소와 일별 관측 레코드를 몽고DB에 저장
import pymongo_spark
pymongo_spark.activate()
cleaned_station_observations.saveToMongoDB('mongodb://localhost:27017/agile_data_science.daily_station_observations')

# 디스크에도 함께 저장
cleaned_station_observations.map(json.dumps).saveAsTextFile("data/daily_station_observations.json")
