import sys, os, re
import json

from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils, OffsetRange, TopicAndPartition

# 10초마다 데이터 처리 
PERIOD=10
BROKERS='localhost:9092'
TOPIC='flight_delay_classification_request'

conf = SparkConf().set("spark.default.parallelism", 1)
# sc = SparkContext(appName = "Agile Data Science: PySpark Streaming 'Hello, World!'", conf=conf)
ssc = StreamingContext(sc, PERIOD)

stream = KafkaUtils.createDirectStream(
  ssc,
  [TOPIC],
  {
    "metadata.broker.list": BROKERS,
    "group.id": "0",
  }
)

# JSON 메시지 파싱하고 결과 객체를 출력
object_stream = stream.map(lambda x: json.loads(x[1]))
object_stream.pprint()

ssc.start()
