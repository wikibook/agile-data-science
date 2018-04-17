csv_lines = sc.textFile("data/example.csv")

from pyspark.sql import Row

# CSV를 pyspark.sql.Row로 변환
def csv_to_row(line):
  parts = line.split(",")
  row = Row(
    name=parts[0],
    company=parts[1],
    title=parts[2]
  )
  return row

# RDD에서 행을 얻기 위해 이 함수 적용
rows = csv_lines.map(csv_to_row)

# pyspark.sql.DataFrame으로 변환
rows_df = rows.toDF()

# Spark SQL을 위해 DataFrame을 등록 
rows_df.registerTempTable("executives")

# SparkSession을 사용해서 SQL로 새 DataFrame 생성
job_counts = spark.sql("""
SELECT
  name,
  COUNT(*) AS total
  FROM executives
  GROUP BY name
""")
job_counts.show()

# RDD로 돌아가기 
job_counts.rdd.collect()
