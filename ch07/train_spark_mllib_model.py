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
  StructField("ArrDelay", DoubleType(), True),     # "ArrDelay":5.0
  StructField("CRSArrTime", TimestampType(), True),    # "CRSArrTime":"2015-12-31T03:20:00.000-08:00"
  StructField("CRSDepTime", TimestampType(), True),    # "CRSDepTime":"2015-12-31T03:05:00.000-08:00"
  StructField("Carrier", StringType(), True),     # "Carrier":"WN"
  StructField("DayOfMonth", IntegerType(), True), # "DayOfMonth":31
  StructField("DayOfWeek", IntegerType(), True),  # "DayOfWeek":4
  StructField("DayOfYear", IntegerType(), True),  # "DayOfYear":365
  StructField("DepDelay", DoubleType(), True),     # "DepDelay":14.0
  StructField("Dest", StringType(), True),        # "Dest":"SAN"
  StructField("Distance", DoubleType(), True),     # "Distance":368.0
  StructField("FlightDate", DateType(), True),    # "FlightDate":"2015-12-30T16:00:00.000-08:00"
  StructField("FlightNum", StringType(), True),   # "FlightNum":"6109"
  StructField("Origin", StringType(), True),      # "Origin":"TUS"
])

features = spark.read.json(
  "data/simple_flight_delay_features.jsonl.bz2",
  schema=schema
)
features.first()

#
# Spark ML을 사용하기 전 특징에 널 값이 있는지 확인
#
null_counts = [(column, features.where(features[column].isNull()).count()) for column in features.columns]
cols_with_nulls = filter(lambda x: x[1] > 0, null_counts)
print(list(cols_with_nulls))

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
features_with_route.select("Origin", "Dest", "Route").show(5)

#
# DataFrame UDF를 사용해 도착 지연 필드를 범주화 혹은 '구간화'
#
def bucketize_arr_delay(arr_delay):
  bucket = None
  if arr_delay <= -15.0:
    bucket = 0.0
  elif arr_delay > -15.0 and arr_delay <= 0.0:
    bucket = 1.0
  elif arr_delay > 0.0 and arr_delay <= 30.0:
    bucket = 2.0
  elif arr_delay > 30.0:
    bucket = 3.0
  return bucket

# 함수를 pyspark.sql.types.StructField 정보를 가지고 pyspark.sql.functions.udf에 감쌈
dummy_function_udf = udf(bucketize_arr_delay, StringType())

# pyspark.sql.DataFrame.withColumn을 통해 범주 열 추가 
manual_bucketized_features = features_with_route.withColumn(
  "ArrDelayBucket",
  dummy_function_udf(features['ArrDelay'])
)
manual_bucketized_features.select("ArrDelay", "ArrDelayBucket").show()

#
#  pyspark.ml.feature.Bucketizer를 사용해 ArrDelay를 구간화
#
from pyspark.ml.feature import Bucketizer

splits = [-float("inf"), -15.0, 0, 30.0, float("inf")]
bucketizer = Bucketizer(
  splits=splits,
  inputCol="ArrDelay",
  outputCol="ArrDelayBucket"
)
ml_bucketized_features = bucketizer.transform(features_with_route)

# 구간 확인
ml_bucketized_features.select("ArrDelay", "ArrDelayBucket").show()

#
# pyspark.ml.feature의 특징 도구를 임포트
#
from pyspark.ml.feature import StringIndexer, VectorAssembler

# 범주 필드를 범주형 속성 벡터로 변환한 다음 중간 필드 제거
for column in ["Carrier", "DayOfMonth", "DayOfWeek", "DayOfYear",
               "Origin", "Dest", "Route"]:
  string_indexer = StringIndexer(
    inputCol=column,
    outputCol=column + "_index"
  )
  ml_bucketized_features = string_indexer.fit(ml_bucketized_features)\
                                          .transform(ml_bucketized_features)

# 인덱스 확인
ml_bucketized_features.show(6)

# 연속형 숫자 필드를 하나의 특징 벡터로 결합해 처리
numeric_columns = ["DepDelay", "Distance"]
index_columns = ["Carrier_index", "DayOfMonth_index",
                   "DayOfWeek_index", "DayOfYear_index", "Origin_index",
                   "Origin_index", "Dest_index", "Route_index"]
vector_assembler = VectorAssembler(
  inputCols=numeric_columns + index_columns,
  outputCol="Features_vec"
)
final_vectorized_features = vector_assembler.transform(ml_bucketized_features)

# 인덱스 열 삭제
for column in index_columns:
  final_vectorized_features = final_vectorized_features.drop(column)

# 특징 확인
final_vectorized_features.show()

#
# 교차 검증, 분류 모델을 훈련시키고 검증하기
#

# 훈련/테스트 데이터 분할
training_data, test_data = final_vectorized_features.randomSplit([0.7, 0.3])

# 랜덤 포레스트 분류 모델을 인스턴스화하고 적합시킴
from pyspark.ml.classification import RandomForestClassifier
rfc = RandomForestClassifier(
  featuresCol="Features_vec",
  labelCol="ArrDelayBucket",
  maxBins=4657,
  maxMemoryInMB=1024
)
model = rfc.fit(training_data)

# 테스트 데이터를 사용해 모델 평가
predictions = model.transform(test_data)

from pyspark.ml.evaluation import MulticlassClassificationEvaluator
evaluator = MulticlassClassificationEvaluator(labelCol="ArrDelayBucket", metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
print("Accuracy = {}".format(accuracy))

# 표본 세너티 체크(sanity check, 적정성 검사)
predictions.sample(False, 0.001, 18).orderBy("CRSDepTime").show(6)
