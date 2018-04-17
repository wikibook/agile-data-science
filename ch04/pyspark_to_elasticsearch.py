# 파케이 파일 적재
on_time_dataframe = spark.read.parquet('data/on_time_performance.parquet')

# DataFrame을 일래스틱서치에 저장
on_time_dataframe.write.format("org.elasticsearch.spark.sql")\
  .option("es.resource","agile_data_science/on_time_performance")\
  .option("es.batch.size.entries","100")\
  .mode("overwrite")\
  .save()

# 일래스틱 서치를 위한 포맷 데이터: 첫 번째 필드에 더미 키를 넣은 튜플임 
# on_time_performance = on_time_dataframe.rdd.map(lambda x: ('ignored_key', x.asDict()))
#
# on_time_performance.saveAsNewAPIHadoopFile(
#   path='-',
#   outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
#   keyClass="org.apache.hadoop.io.NullWritable",
#   valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
#   conf={ "es.resource" : "agile_data_science/on_time_performance" })
