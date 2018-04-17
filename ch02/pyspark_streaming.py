import sys, os, re
import json

from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils, OffsetRange, TopicAndPartition

# 데이터를 10초마다 처리
PERIOD=10
BROKERS='localhost:9092'
TOPIC='test'

conf = SparkConf().set("spark.default.parallelism", 1)
#sc = SparkContext(appName = "Agile Data Science: PySpark Streaming 'Hello, World!'", conf = conf)
ssc = StreamingContext(sc, PERIOD)

stream = KafkaUtils.createDirectStream(
  ssc,
  [TOPIC],
  {
    "metadata.broker.list": BROKERS,
    "group.id": "0",
  }
)
object_stream = stream.map(lambda x: json.loads(x[1]))
object_stream.pprint()

ssc.start()
