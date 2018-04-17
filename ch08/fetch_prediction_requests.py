#!/usr/bin/env python

import sys, os, re
import json
import datetime, iso8601

# 몽고DB로 저장
from bson import json_util
import pymongo_spark
pymongo_spark.activate()

# airflow에서 날짜와 기본 경로를 main()으로 전달
def main(iso_date, base_path):
  
  APP_NAME = "fetch_prediction_requests.py"
  
  # SparkSession이 없으면 환경 생성
  try:
    sc and spark
  except NameError as e:
    import findspark
    findspark.init()
    import pyspark
    import pyspark.sql
    
    sc = pyspark.SparkContext()
    spark = pyspark.sql.SparkSession(sc).builder.appName(APP_NAME).getOrCreate()
  
  # 쿼리 범위를 정하기 위해 오늘과 내일 날짜를 ISO 문자열로 계산
  today_dt = iso8601.parse_date(iso_date)
  rounded_today = today_dt.date()
  iso_today = rounded_today.isoformat()
  rounded_tomorrow_dt = rounded_today + datetime.timedelta(days=1)
  iso_tomorrow = rounded_tomorrow_dt.isoformat()
  
  # 오늘 데이터를 위한 몽고 쿼리 문자열 생성
  mongo_query_string = """{{
    "Timestamp": {{
      "$gte": "{iso_today}",
      "$lte": "{iso_tomorrow}"
    }}
  }}""".format(
    iso_today=iso_today,
    iso_tomorrow=iso_tomorrow
  )
  mongo_query_string = mongo_query_string.replace('\n', '')
  
  # 쿼리 문자열로 설정 객체 생성
  mongo_query_config = dict()
  mongo_query_config["mongo.input.query"] = mongo_query_string
  
  # pymongo_spark를 사용해 해당 날짜의 요청을 적재
  prediction_requests = sc.mongoRDD(
    'mongodb://localhost:27017/agile_data_science.prediction_tasks',
    config=mongo_query_config
  )
  
  #  특정일의 출력 경로 생성: 날짜 기반의 프라이머리 키 디렉터리 구조
  today_output_path = "{}/data/prediction_tasks_daily.json/{}".format(
    base_path,
    iso_today
  )
  
  # JSON 레코드 생성
  prediction_requests_json = prediction_requests.map(json_util.dumps)
  
  # 오늘에 해당하는 출력 경로 쓰기/대체하기
  os.system("rm -rf {}".format(today_output_path))
  prediction_requests_json.saveAsTextFile(today_output_path)

if __name__ == "__main__":
  main(sys.argv[1], sys.argv[2])
