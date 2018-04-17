#!/usr/bin/env python

import sys, os, re
import json
import datetime, iso8601

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, Row
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

# 몽고DB에 저장
from bson import json_util
import pymongo_spark
pymongo_spark.activate()

def main(base_path):

  APP_NAME = "make_predictions_streaming.py"

  # 10초마다 데이터 처리
  PERIOD = 10
  BROKERS = 'localhost:9092'
  PREDICTION_TOPIC = 'flight_delay_classification_request'
  
  try:
    sc and ssc
  except NameError as e:
    import findspark

    # 스트리밍 패키지 추가 및 초기화 
    findspark.add_packages(["org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.0"])
    findspark.init()
    
    import pyspark
    import pyspark.sql
    import pyspark.streaming
  
    conf = SparkConf().set("spark.default.parallelism", 1)
    sc = SparkContext(appName="Agile Data Science: PySpark Streaming 'Hello, World!'", conf=conf)
    ssc = StreamingContext(sc, PERIOD)
    spark = pyspark.sql.SparkSession(sc).builder.appName(APP_NAME).getOrCreate()
  
  #
  # 예측 생성에 사용된 모든 모델 적재
  #
  
  # 도착 지연 구간화 모델 적재
  from pyspark.ml.feature import Bucketizer
  arrival_bucketizer_path = "{}/models/arrival_bucketizer_2.0.bin".format(base_path)
  arrival_bucketizer = Bucketizer.load(arrival_bucketizer_path)
  
  # 모든 문자열 필드 벡터화 파이프라인을 dict에 적재
  from pyspark.ml.feature import StringIndexerModel
  
  string_indexer_models = {}
  for column in ["Carrier", "Origin", "Dest", "Route"]:
    string_indexer_model_path = "{}/models/string_indexer_model_{}.bin".format(
      base_path,
      column
    )
    string_indexer_model = StringIndexerModel.load(string_indexer_model_path)
    string_indexer_models[column] = string_indexer_model

  # 숫자 벡터 어셈블러 적재
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
  # 스트리밍에서 예측 요청 처리
  #
  stream = KafkaUtils.createDirectStream(
    ssc,
    [PREDICTION_TOPIC],
    {
      "metadata.broker.list": BROKERS,
      "group.id": "0",
    }
  )

  object_stream = stream.map(lambda x: json.loads(x[1]))
  object_stream.pprint()
  
  row_stream = object_stream.map(
    lambda x: Row(
      FlightDate=iso8601.parse_date(x['FlightDate']),
      Origin=x['Origin'],
      Distance=x['Distance'],
      DayOfMonth=x['DayOfMonth'],
      DayOfYear=x['DayOfYear'],
      UUID=x['UUID'],
      DepDelay=x['DepDelay'],
      DayOfWeek=x['DayOfWeek'],
      FlightNum=x['FlightNum'],
      Dest=x['Dest'],
      Timestamp=iso8601.parse_date(x['Timestamp']),
      Carrier=x['Carrier']
    )
  )
  row_stream.pprint()

  #
  # RDD 기반 객체 스트림에서 dataframe 생성
  #

  def classify_prediction_requests(rdd):
  
    from pyspark.sql.types import StringType, IntegerType, DoubleType, DateType, TimestampType
    from pyspark.sql.types import StructType, StructField
  
    prediction_request_schema = StructType([
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
      StructField("UUID", StringType(), True),
    ])
    
    prediction_requests_df = spark.createDataFrame(rdd, schema=prediction_request_schema)
    prediction_requests_df.show()

    #
    # FlightNum을 대체할 Route 변수 추가
    #

    from pyspark.sql.functions import lit, concat
    prediction_requests_with_route = prediction_requests_df.withColumn(
      'Route',
      concat(
        prediction_requests_df.Origin,
        lit('-'),
        prediction_requests_df.Dest
      )
    )
    prediction_requests_with_route.show(6)
  
    # 문자열 필드를 해당 열에 대응하는 파이프라인으로 벡터화
    # 범주 필드를 범주형 특징 벡터로 변환한 다음 중간 결과 필드 삭제
    for column in ["Carrier", "Origin", "Dest", "Route"]:
      string_indexer_model = string_indexer_models[column]
      prediction_requests_with_route = string_indexer_model.transform(prediction_requests_with_route)
  
    # 숫사 열 벡터화: DepDelay, Distance, 인덱스 열
    final_vectorized_features = vector_assembler.transform(prediction_requests_with_route)
    
    # 벡터 검사
    final_vectorized_features.show()
  
    # 개별 인덱스 열 제거
    index_columns = ["Carrier_index", "Origin_index", "Dest_index", "Route_index"]
    for column in index_columns:
      final_vectorized_features = final_vectorized_features.drop(column)
  
    # 확정된 특징 검사
    final_vectorized_features.show()
  
    # 예측 생성
    predictions = rfc.transform(final_vectorized_features)
  
    # 원 필드에 제공하기 위해 특징 벡터와 예측 메타데이터 제거
    predictions = predictions.drop("Features_vec")
    final_predictions = predictions.drop("indices").drop("values").drop("rawPrediction").drop("probability")
  
    # 결과 검사
    final_predictions.show()
  
    # 몽고DB에 저장
    if final_predictions.count() > 0:
      final_predictions.rdd.map(lambda x: x.asDict()).saveToMongoDB(
        "mongodb://localhost:27017/agile_data_science.flight_delay_classification_response"
      )
  
  # 분류를 수행하고 몽고 DB에 저장
  row_stream.foreachRDD(classify_prediction_requests)
  
  ssc.start()
  ssc.awaitTermination()

if __name__ == "__main__":
  main(sys.argv[1])
