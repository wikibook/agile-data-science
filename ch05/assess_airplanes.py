# 파케이 파일 적재
on_time_dataframe = spark.read.parquet('data/on_time_performance.parquet')
on_time_dataframe.registerTempTable("on_time_performance")

# 불필요한 필드 제거
tail_numbers = on_time_dataframe.rdd.map(lambda x: x.TailNum)
tail_numbers = tail_numbers.filter(lambda x: x != '')

# distinct()를 사용해서 고유의 꼬리 번호 가져오기
unique_tail_numbers = tail_numbers.distinct()

# 이제 고유 꼬리 번호의 count()가 필요함
airplane_count = unique_tail_numbers.count()
print("Total airplanes: {}".format(airplane_count))