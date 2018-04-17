#!/usr/bin/env python

import sys, os, re
import json
import datetime, iso8601

# airflow에서 날짜와 기본 경로를 main()으로 전달
def main(base_path):
  
  APP_NAME = "extract_features.py"
  
  # SparkSession이 없으면 환경 설정
  try:
    sc and spark
  except NameError as e:
    import findspark
    findspark.init()
    import pyspark
    import pyspark.sql
    
    sc = pyspark.SparkContext()
    spark = pyspark.sql.SparkSession(sc).builder.appName(APP_NAME).getOrCreate()
  
  # 정시 운항 실적 파케이 파일 적재
  input_path = "{}/data/on_time_performance.parquet".format(
    base_path
  )
  on_time_dataframe = spark.read.parquet(input_path)
  on_time_dataframe.registerTempTable("on_time_performance")
  
  # 관심 있는 몇 가지 특징 선택
  simple_on_time_features = spark.sql("""
  SELECT
    FlightNum,
    FlightDate,
    DayOfWeek,
    DayofMonth AS DayOfMonth,
    CONCAT(Month, '-',  DayofMonth) AS DayOfYear,
    Carrier,
    Origin,
    Dest,
    Distance,
    DepDelay,
    ArrDelay,
    CRSDepTime,
    CRSArrTime,
    CONCAT(Origin, '-', Dest) AS Route
  FROM on_time_performance
  """)
  simple_on_time_features.show()
  
  # 도움이 되지 않는 널 값은 필터링
  filled_on_time_features = simple_on_time_features.filter(
    (simple_on_time_features.ArrDelay != None)
    &
    (simple_on_time_features.DepDelay != None)
  )
  
  # 타임스탬프를 문자열이나 숫자가 아닌 타임스탬프로 전환해야 함
  def convert_hours(hours_minutes):
    hours = hours_minutes[:-2]
    minutes = hours_minutes[-2:]
  
    if hours == '24':
      hours = '23'
      minutes = '59'
  
    time_string = "{}:{}:00Z".format(hours, minutes)
    return time_string
  
  def compose_datetime(iso_date, time_string):
    return "{} {}".format(iso_date, time_string)
  
  def create_iso_string(iso_date, hours_minutes):
    time_string = convert_hours(hours_minutes)
    full_datetime = compose_datetime(iso_date, time_string)
    return full_datetime
  
  def create_datetime(iso_string):
    return iso8601.parse_date(iso_string)
  
  def convert_datetime(iso_date, hours_minutes):
    iso_string = create_iso_string(iso_date, hours_minutes)
    dt = create_datetime(iso_string)
    return dt
  
  def day_of_year(iso_date_string):
    dt = iso8601.parse_date(iso_date_string)
    doy = dt.timetuple().tm_yday
    return doy
  
  def alter_feature_datetimes(row):
  
    flight_date = iso8601.parse_date(row['FlightDate'])
    scheduled_dep_time = convert_datetime(row['FlightDate'], row['CRSDepTime'])
    scheduled_arr_time = convert_datetime(row['FlightDate'], row['CRSArrTime'])
  
    # 야간 운항 처리
    if scheduled_arr_time < scheduled_dep_time:
      scheduled_arr_time += datetime.timedelta(days=1)
  
    doy = day_of_year(row['FlightDate'])
  
    return {
      'FlightNum': row['FlightNum'],
      'FlightDate': flight_date,
      'DayOfWeek': int(row['DayOfWeek']),
      'DayOfMonth': int(row['DayOfMonth']),
      'DayOfYear': doy,
      'Carrier': row['Carrier'],
      'Origin': row['Origin'],
      'Dest': row['Dest'],
      'Distance': row['Distance'],
      'DepDelay': row['DepDelay'],
      'ArrDelay': row['ArrDelay'],
      'CRSDepTime': scheduled_dep_time,
      'CRSArrTime': scheduled_arr_time,
      'Route': row['Route'],
    }
  
  timestamp_features = filled_on_time_features.rdd.map(alter_feature_datetimes)
  timestamp_df = timestamp_features.toDF()
  
  # 명시적으로 데이터를 정렬하고 그 정렬 상태를 유지. 운에 맡기지 말 것.
  sorted_features = timestamp_df.sort(
    timestamp_df.DayOfYear,
    timestamp_df.Carrier,
    timestamp_df.Origin,
    timestamp_df.Dest,
    timestamp_df.FlightNum,
    timestamp_df.CRSDepTime,
    timestamp_df.CRSArrTime,
  )
  
  # 단일 json 파일로 저장
  output_path = "{}/data/simple_flight_delay_features.json".format(
    base_path
  )
  sorted_features.repartition(1).write.mode("overwrite").json(output_path)
  
  # 부분 파일을 JSON Lines 파일에 복사
  combine_cmd = "cp {}/part* {}/data/simple_flight_delay_features.jsonl".format(
    output_path,
    base_path
  )
  os.system(combine_cmd)

if __name__ == "__main__":
  main(sys.argv[1])

