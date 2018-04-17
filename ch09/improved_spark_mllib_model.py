# !/usr/bin/env python

import sys, os, re
import json
import datetime, iso8601
from tabulate import tabulate

# airflow에서 날짜와 기본 경로를 main()으로 전달
def main(base_path):
  APP_NAME = "train_spark_mllib_model.py"
  
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
  # {
  #   "ArrDelay":5.0,"CRSArrTime":"2015-12-31T03:20:00.000-08:00","CRSDepTime":"2015-12-31T03:05:00.000-08:00",
  #   "Carrier":"WN","DayOfMonth":31,"DayOfWeek":4,"DayOfYear":365,"DepDelay":14.0,"Dest":"SAN","Distance":368.0,
  #   "FlightDate":"2015-12-30T16:00:00.000-08:00","FlightNum":"6109","Origin":"TUS"
  # }
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
  
  input_path = "{}/data/simple_flight_delay_features.json".format(
    base_path
  )
  features = spark.read.json(input_path, schema=schema)
  features.first()
  
  #
  # FlightNum을 대체할 Route 변수 추가
  #
  from pyspark.sql.functions import lit, concat
  features_with_route = features.withColumn(
    'Route',
    concat(
      features.Origin,
      lit('-'),
      features.Dest
    )
  )
  features_with_route.show(6)
  
  #
  # 예정된 도착/출발 시간 추가
  #
  from pyspark.sql.functions import hour
  features_with_hour = features_with_route.withColumn(
    "CRSDepHourOfDay",
    hour(features.CRSDepTime)
  )
  features_with_hour = features_with_hour.withColumn(
    "CRSArrHourOfDay",
    hour(features.CRSArrTime)
  )
  features_with_hour.select("CRSDepTime", "CRSDepHourOfDay", "CRSArrTime", "CRSArrHourOfDay").show()

  #
  # pysmark.ml.feature.Bucketizer를 사용해서 ArrDelay를 on-time, slightly late, very late (0, 1, 2)으로 구간화
  #
  from pyspark.ml.feature import Bucketizer

  # 구간화 모델 설정
  splits = [-float("inf"), -15.0, 0, 30.0, float("inf")]
  arrival_bucketizer = Bucketizer(
    splits=splits,
    inputCol="ArrDelay",
    outputCol="ArrDelayBucket"
  )

  # 모델 저장
  arrival_bucketizer_path = "{}/models/arrival_bucketizer_2.0.bin".format(base_path)
  arrival_bucketizer.write().overwrite().save(arrival_bucketizer_path)

  # 모델 적용
  ml_bucketized_features = arrival_bucketizer.transform(features_with_hour)
  ml_bucketized_features.select("ArrDelay", "ArrDelayBucket").show()

  #
  # pyspark.ml.feature의 특징 도구 임포트
  #
  from pyspark.ml.feature import StringIndexer, VectorAssembler

  # 범주 필드를 인덱스로 전환
  for column in ["Carrier", "Origin", "Dest", "Route"]:
    string_indexer = StringIndexer(
      inputCol=column,
      outputCol=column + "_index"
    )
  
    string_indexer_model = string_indexer.fit(ml_bucketized_features)
    ml_bucketized_features = string_indexer_model.transform(ml_bucketized_features)
  
    # 파이프라인 모델 저장
    string_indexer_output_path = "{}/models/string_indexer_model_3.0.{}.bin".format(
      base_path,
      column
    )
    string_indexer_model.write().overwrite().save(string_indexer_output_path)

  # 연속형 수치 필드를 명목형 필드의 인덱스와 결합해서 하나의 특징 벡터로 만듦
  numeric_columns = [
    "DepDelay", "Distance",
    "DayOfMonth", "DayOfWeek",
    "DayOfYear", "CRSDepHourOfDay",
    "CRSArrHourOfDay"]
  index_columns = ["Carrier_index", "Origin_index",
                   "Dest_index", "Route_index"]
  vector_assembler = VectorAssembler(
    inputCols=numeric_columns + index_columns,
    outputCol="Features_vec"
  )
  final_vectorized_features = vector_assembler.transform(ml_bucketized_features)

  # 수치 벡터 어셈블러를 저장
  vector_assembler_path = "{}/models/numeric_vector_assembler_3.0.bin".format(base_path)
  vector_assembler.write().overwrite().save(vector_assembler_path)

  # 인덱스 열 제거
  for column in index_columns:
    final_vectorized_features = final_vectorized_features.drop(column)

  # 확정된 특징을 검사
  final_vectorized_features.show()

  #
  # 분류 모델을 교차 검증, 훈련, 평가: 4개의 지표에 대해 5회 반복
  #

  from collections import defaultdict
  scores = defaultdict(list)
  feature_importances = defaultdict(list)
  metric_names = ["accuracy", "weightedPrecision", "weightedRecall", "f1"]
  split_count = 3

  for i in range(1, split_count + 1):
    print("\nRun {} out of {} of test/train splits in cross validation...".format(
        i,
        split_count,
      )
    )
  
    # 훈련/테스트 데이터 분할
    training_data, test_data = final_vectorized_features.randomSplit([0.8, 0.2])
  
    # 전체 데이터에 대해 랜덤 포레스트 분류 모델을 인스턴스화하고 적합시킴
    from pyspark.ml.classification import RandomForestClassifier
    rfc = RandomForestClassifier(
      featuresCol="Features_vec",
      labelCol="ArrDelayBucket",
      predictionCol="Prediction",
      maxBins=4657,
    )
    model = rfc.fit(training_data)
  
    # 예전 모델 대신 새 모델을 저장
    model_output_path = "{}/models/spark_random_forest_classifier.flight_delays.baseline.bin".format(
      base_path
    )
    model.write().overwrite().save(model_output_path)
  
    # 테스트 데이터로 모델을 평가
    predictions = model.transform(test_data)
  
    # 이 테스트/훈련 데이터 분할의 결과를 각 지표별로평가
    from pyspark.ml.evaluation import MulticlassClassificationEvaluator
    for metric_name in metric_names:
      evaluator = MulticlassClassificationEvaluator(
        labelCol="ArrDelayBucket",
        predictionCol="Prediction",
        metricName=metric_name
      )
      score = evaluator.evaluate(predictions)
    
      scores[metric_name].append(score)
      print("{} = {}".format(metric_name, score))

    #
    # 특징 중요도 수집
    #
    feature_names = vector_assembler.getInputCols()
    feature_importance_list = model.featureImportances
    for feature_name, feature_importance in zip(feature_names, feature_importance_list):
      feature_importances[feature_name].append(feature_importance)

  #
  # 지표별 평균과 표준편차 평가 및 표로 출력
  #
  import numpy as np
  score_averages = defaultdict(float)
  
  # 표 데이터 계산
  average_stds = [] # ha
  for metric_name in metric_names:
    metric_scores = scores[metric_name]
    
    average_accuracy = sum(metric_scores) / len(metric_scores)
    score_averages[metric_name] = average_accuracy
    
    std_accuracy = np.std(metric_scores)
    
    average_stds.append((metric_name, average_accuracy, std_accuracy))
  
  # 표 출력
  print("\nExperiment Log")
  print("--------------")
  print(tabulate(average_stds, headers=["Metric", "Average", "STD"]))
  
  #
  # 점수를 실행 사이에 존재하는 점수 로그에 유지
  #
  import pickle
  
  # 점수 로그를 적재하거나 빈 로그를 초기화
  try:
    score_log_filename = "{}/models/score_log.pickle".format(base_path)
    score_log = pickle.load(open(score_log_filename, "rb"))
    if not isinstance(score_log, list):
      score_log = []
  except IOError:
    score_log = []
  
  #  기존 점수 로그 계산
  score_log_entry = {metric_name: score_averages[metric_name] for metric_name in metric_names}
  
  # 각 지표에 대한 점수 변화를 계산하고 디스플레이
  try:
    last_log = score_log[-1]
  except (IndexError, TypeError, AttributeError):
    last_log = score_log_entry
  
  experiment_report = []
  for metric_name in metric_names:
    run_delta = score_log_entry[metric_name] - last_log[metric_name]
    experiment_report.append((metric_name, run_delta))
  
  print("\nExperiment Report")
  print("-----------------")
  print(tabulate(experiment_report, headers=["Metric", "Score"]))
  
  # 기존 평균 점수를 로그에 추가
  score_log.append(score_log_entry)
  
  # Persist the log for next run
  pickle.dump(score_log, open(score_log_filename, "wb"))
  
  #
  # 특징 중요도 변화를 분석하고 보고
  #
  
  # 각 특징에 대한 평균 계산
  feature_importance_entry = defaultdict(float)
  for feature_name, value_list in feature_importances.items():
    average_importance = sum(value_list) / len(value_list)
    feature_importance_entry[feature_name] = average_importance
  
  # 특징 중요도를 내림차순으로 정렬하고 출력
  import operator
  sorted_feature_importances = sorted(
    feature_importance_entry.items(),
    key=operator.itemgetter(1),
    reverse=True
  )
  
  print("\nFeature Importances")
  print("-------------------")
  print(tabulate(sorted_feature_importances, headers=['Name', 'Importance']))
  
  #
  # 이번 실행 결과인 특징 중요도를 이전 실행 결과와 비교
  #
    
  # 특징 중요도 로그를 적재하거나 빈 로그를 초기화
  try:
    feature_log_filename = "{}/models/feature_log.pickle".format(base_path)
    feature_log = pickle.load(open(feature_log_filename, "rb"))
    if not isinstance(feature_log, list):
      feature_log = []
  except IOError:
    feature_log = []
  
  # 각 특징에 대한 점수 변화를 계산하고 디스플레이
  try:
    last_feature_log = feature_log[-1]
  except (IndexError, TypeError, AttributeError):
    last_feature_log = defaultdict(float)
    for feature_name, importance in feature_importance_entry.items():
      last_feature_log[feature_name] = importance

  # 변동 값 계산
  feature_deltas = {}
  for feature_name in feature_importances.keys():
    run_delta = feature_importance_entry[feature_name] - last_feature_log[feature_name]
    feature_deltas[feature_name] = run_delta
  
  # 특징 변동 값을 정렬해 가장 큰 변동이 있는 특징을 먼저 나오게 한다
  import operator
  sorted_feature_deltas = sorted(
    feature_deltas.items(),
    key=operator.itemgetter(1),
    reverse=True
  )
  
  # 정렬된 특징 변동 값 디스플레이
  print("\nFeature Importance Delta Report")
  print("-------------------------------")
  print(tabulate(sorted_feature_deltas, headers=["Feature", "Delta"]))

  # 로그에 기존 평균 변동 값을 추가
  feature_log.append(feature_importance_entry)

  # 다음 실행을 위해 로그 유지
  pickle.dump(feature_log, open(feature_log_filename, "wb"))
  
if __name__ == "__main__":
  main(sys.argv[1])
