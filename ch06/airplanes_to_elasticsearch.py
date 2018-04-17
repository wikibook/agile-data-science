# 항공기 데이터 적재
airplanes = spark.read.json("data/airplanes.json")
airplanes.show()

airplanes.write.format("org.elasticsearch.spark.sql")\
  .option("es.resource","agile_data_science/airplane")\
  .mode("overwrite")\
  .save()

# 일래스틱서치를 위한 포맷 데이터, 첫 번째 필드에 더미 키를 넣은 튜플임
# airplanes_dict = airplanes.rdd.map(lambda x: ('ignored_key', x.asDict()))
#
# airplanes_dict.saveAsNewAPIHadoopFile(
#   path='-',
#   outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
#   keyClass="org.apache.hadoop.io.NullWritable",
#   valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
#   conf={ "es.resource" : "agile_data_science/airplanes" })
