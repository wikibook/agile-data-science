# 파케이 파일 적재
on_time_dataframe = spark.read.parquet('data/on_time_performance.parquet')
on_time_dataframe.registerTempTable("on_time_performance")

# 비행을 식별하고 비행에 링크를 걸기 위해 필요한 필드만 필터링
flights = on_time_dataframe.rdd.map(lambda x: 
  (x.Carrier, x.FlightDate, x.FlightNum, x.Origin, x.Dest, x.TailNum)
  )

# 비행을 꼬리 번호(tail number) 별로 그룹화, 날짜(date), 운항 번호(flight number), 출발지(origin)/목적지(dest) 순서대로 정렬
flights_per_airplane = flights\
  .map(lambda nameTuple: (nameTuple[5], [nameTuple[0:5]]))\
  .reduceByKey(lambda a, b: a + b)\
  .map(lambda tuple:
      {
        'TailNum': tuple[0], 
        'Flights': sorted(tuple[1], key=lambda x: (x[1], x[2], x[3], x[4]))
      }
    )
flights_per_airplane.first()

# 몽고에 저장
import pymongo_spark
pymongo_spark.activate()
flights_per_airplane.saveToMongoDB('mongodb://localhost:27017/agile_data_science.flights_per_airplane')

