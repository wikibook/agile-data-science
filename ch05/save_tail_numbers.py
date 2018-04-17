#  파케이 파일 적재
on_time_dataframe = spark.read.parquet('data/on_time_performance.parquet')
on_time_dataframe.registerTempTable("on_time_performance")

# 불필요한 필드 제거
tail_numbers = on_time_dataframe.rdd.map(lambda x: x.TailNum)
tail_numbers = tail_numbers.filter(lambda x: x != '')

# distinct()를 사용해서 고유의 꼬리 번호를 가져옴
unique_tail_numbers = tail_numbers.distinct()

# dataframe을 통해 JSON 객체로 저장함. 한 개의 json 파일을 가져오기 위해 repartition을 1로 설정함
unique_records = unique_tail_numbers.map(lambda x: {'TailNum': x})
unique_records.toDF().repartition(1).write.json("data/tail_numbers.json")

# 이제 bash에서 실행하면 됨: ls data/tail_numbers.json/part*
