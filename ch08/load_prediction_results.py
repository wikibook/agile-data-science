#!/usr/bin/env python

import sys, os, re
import json
import datetime, iso8601

# 몽고DB에 저장
from bson import json_util
import pymongo_spark
pymongo_spark.activate()

# airflow에서 날짜와 기본 경로를 main()으로 전달
def main(iso_date, base_path):
  
  APP_NAME = "load_prediction_results.py"
  
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
  
  #  쿼리 범위를 정하기 위해 오늘과 내일 날짜를 ISO 문자열로 구함
  today_dt = iso8601.parse_date(iso_date)
  rounded_today = today_dt.date()
  iso_today = rounded_today.isoformat()
  
  input_path = "{}/data/prediction_results_daily.json/{}".format(
    base_path,
    iso_today
  )
  
  # 텍스트 적재 및 JSON으로 변환
  prediction_results_raw = sc.textFile(input_path)
  prediction_results = prediction_results_raw.map(json_util.loads)
  
  # 몽고DB에 저장
  prediction_results.saveToMongoDB(
    "mongodb://localhost:27017/agile_data_science.prediction_results"
  )

if __name__ == "__main__":
  main(sys.argv[1], sys.argv[2])
