# 정시 운항 실적 파케이 파일 적재
on_time_dataframe = spark.read.parquet('data/on_time_performance.parquet')

# 첫 번째 단계는 SQL로 쉽게 표현됨: 각 항공사에 소속된 고유의 꼬리 번호를 모두 가져옴
on_time_dataframe.registerTempTable("on_time_performance")
carrier_airplane = spark.sql(
  "SELECT DISTINCT Carrier, TailNum FROM on_time_performance"
  )

# 이제 항공사별 꼬리 번호의 정렬된 목록을 항공기 수량과 함께 저장해야 함
airplanes_per_carrier = carrier_airplane.rdd\
  .map(lambda nameTuple: (nameTuple[0], [nameTuple[1]]))\
  .reduceByKey(lambda a, b: a + b)\
  .map(lambda tuple:
      {
        'Carrier': tuple[0], 
        'TailNumbers': sorted(
          filter(
            lambda x: x is not None and x != '', tuple[1] # 꼬리 번호가 빈 문자열인 경우 지나감
            )
          ),
        'FleetCount': len(tuple[1])
      }
    )
airplanes_per_carrier.count() # 14

# airplanes_per_carrier 관계에서 몽고DB로 저장
import pymongo_spark
pymongo_spark.activate()
airplanes_per_carrier.saveToMongoDB(
  'mongodb://localhost:27017/agile_data_science.airplanes_per_carrier'
)
