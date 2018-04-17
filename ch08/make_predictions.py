#!/usr/bin/env python

import sys, os, re
import json
import datetime, iso8601

# 에어플로우에서 날짜와 기본 경로를 main()에 전달
def main(iso_date, base_path):

  APP_NAME = "make_predictions.py"
  
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
  
  #
  # 파이프라인에 모든 모델을 적재
  #
  
  # 도착 지연 구간 설정 모델을 적재
  from pyspark.ml.feature import Bucketizer
  arrival_bucketizer_path = "{}/models/arrival_bucketizer_2.0.bin".format(base_path)
  arrival_bucketizer = Bucketizer.load(arrival_bucketizer_path)
  
  # 모든 문자열 인덱서를 dict에 적재
  from pyspark.ml.feature import StringIndexerModel
  
  string_indexer_models = {}
  for column in ["Carrier", "DayOfMonth", "DayOfWeek", "DayOfYear",
                 "Origin", "Dest", "Route"]:
    string_indexer_model_path = "{}/models/string_indexer_model_{}.bin".format(
      base_path,
      column
    )
    string_indexer_model = StringIndexerModel.load(string_indexer_model_path)
    string_indexer_models[column] = string_indexer_model
  
  # 수치 벡터 어셈블러 적재
  from pyspark.ml.feature import VectorAssembler
  vector_assembler_path = "{}/models/numeric_vector_assembler.bin".format(base_path)
  vector_assembler = VectorAssembler.load(vector_assembler_path)
    
  # 분류 모델 적재
  from pyspark.ml.classification import RandomForestClassifier, RandomForestClassificationModel
  random_forest_model_path = "{}/models/spark_random_forest_classifier.flight_delays.5.0.bin".format(
      base_path
  )
  rfc = RandomForestClassificationModel.load(
    random_forest_model_path
  )
  
  #
  # 요청을 훈련 데이터로부터 변환을 통해 실행
  #
  
  # 쿼리 범위를 지정하기 위해 ISO 문자열로 오늘과 내일 날짜 가져오기
  today_dt = iso8601.parse_date(iso_date)
  rounded_today = today_dt.date()쿼리 범위를 지정하기 위해 ISO 문자열로 오늘과 내일 날짜 가져오기
  iso_today = rounded_today.isoformat()

  # 해당 날짜의 입력 경로 생성: 날짜 기반의 프라이머리 키 디렉터리 구조
  today_input_path = "{}/data/prediction_tasks_daily.json/{}".format(
    base_path,
    iso_today
  )

  from pyspark.sql.types import StringType, IntegerType, DoubleType, DateType, TimestampType
  from pyspark.sql.types import StructType, StructField

  schema = StructType([
    StructField("Carrier", StringType(), True),
    StructField("DayOfMonth", IntegerType(), True),
    StructField("DayOfWeek", IntegerType(), True),
    StructField("DayOfYear", IntegerType(), True),
    StructField("DepDelay", DoubleType(), True),
    StructField("Dest", StringType(), True),
    StructField("Distance", DoubleType(), True),
    StructField("FlightDate", DateType(), True),
    StructField("FlightNum", StringType(), True),
    StructField("Origin", StringType(), True),
    StructField("Timestamp", TimestampType(), True),
  ])
  
  prediction_requests = spark.read.json(today_input_path, schema=schema)
  prediction_requests.show()

  #
  # FlightNum을 대체할 Route 변수 추가
  #
  
  from pyspark.sql.functions import lit, concat
  prediction_requests_with_route = prediction_requests.withColumn(
    'Route',
    concat(
      prediction_requests.Origin,
      lit('-'),
      prediction_requests.Dest
    )
  )
  prediction_requests_with_route.show(6)
  
  #  해당 열에 대응하는 인덱서로 문자열 필드를 인덱싱
  for column in ["Carrier", "DayOfMonth", "DayOfWeek", "DayOfYear",
                 "Origin", "Dest", "Route"]:
    string_indexer_model = string_indexer_models[column]
    prediction_requests_with_route = string_indexer_model.transform(prediction_requests_with_route)
      
  # 수치열 벡터화: DepDelay, Distance
  final_vectorized_features = vector_assembler.transform(prediction_requests_with_route)
  
  # 명목형 필드를 위한 인덱스 제거
  index_columns = ["Carrier_index", "DayOfMonth_index","DayOfWeek_index",
                   "DayOfYear_index", "Origin_index", "Origin_index",
                   "Dest_index", "Route_index"]
  for column in index_columns:
    final_vectorized_features = final_vectorized_features.drop(column)

  # 확정된 특징 검사
  final_vectorized_features.show()
  
  # 예측 생성
  predictions = rfc.transform(final_vectorized_features)
  
  # 원래 필드를 제공하기 위해 특징 벡터와 예측 메타데이터를 제거
  predictions = predictions.drop("Features_vec")
  final_predictions = predictions.drop("indices").drop("values").drop("rawPrediction").drop("probability")
  
  # 결과 검사
  final_predictions.show()
  
  # 해당 날짜의 경로 생성: 날짜 기반의 프라이머리 키 디렉터리 구조
  today_output_path = "{}/data/prediction_results_daily.json/{}".format(
    base_path,
    iso_today
  )
  
  # 일별 구간에 결과 저장
  final_predictions.repartition(1).write.mode("overwrite").json(today_output_path)

if __name__ == "__main__":
  main(sys.argv[1], sys.argv[2])
