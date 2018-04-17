#!/usr/bin/env python

import sys, os, re
import json
import datetime, iso8601

# 날짜와 기본 경로를 에어플로우에서 main()으로 전달
def main(iso_date, base_path):
  APP_NAME = "pyspark_task_one.py"
  
  # SparkSession이 없다면 그 환경을 생성
  try:
    sc and spark
  except NameError as e:
    import findspark
    findspark.init()
    import pyspark
    import pyspark.sql
    
    sc = pyspark.SparkContext()
    spark = pyspark.sql.SparkSession(sc).builder.appName(APP_NAME).getOrCreate()

  # 오늘 날짜 가져오기
  today_dt = iso8601.parse_date(iso_date)
  rounded_today = today_dt.date()

  # 오늘 날짜 적재
  today_input_path = "{}/ch02/data/example_name_titles_daily.json/{}".format(
    base_path,
    rounded_today.isoformat()
  )

  # 데이터를 적재하고 계속 진행
  people_titles = spark.read.json(today_input_path)
  people_titles.show()
  
  # RDD로 Group by
  titles_by_name = people_titles.rdd.groupBy(lambda x: x["name"])
  
  # 그룹키와 그룹화된 데이터를 받아서 다양한 직함을 마스터 직함으로 연결
  def concatenate_titles(people_titles):
    name = people_titles[0]
    title_records = people_titles[1]
    master_title = ""
    for title_record in sorted(title_records):
      title = title_record["title"]
      master_title += "{}, ".format(title)
    master_title = master_title[:-2]
    record = {"name": name, "master_title": master_title}
    return record
  
  people_with_contactenated_titles = titles_by_name.map(concatenate_titles)
  people_output_json = people_with_contactenated_titles.map(json.dumps)
  
  # 오늘의 출력 경로 가져오기
  today_output_path = "{}/ch02/data/example_master_titles_daily.json/{}".format(
    base_path,
    rounded_today.isoformat()
  )
  
  # 오늘의 출력 경로 쓰기/대체하기
  os.system("rm -rf {}".format(today_output_path))
  people_output_json.saveAsTextFile(today_output_path)

if __name__ == "__main__":
  main(sys.argv[1], sys.argv[2])
