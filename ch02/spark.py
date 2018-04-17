# SparkContext를 사용해서 텍스트 파일 적재
csv_lines = sc.textFile("data/example.csv")

# 줄을 목록으로 나누기 위해 데이터를 매핑
data = csv_lines.map(lambda line: line.split(","))

# 데이터셋을 로컬 RAM으로 수집
data.collect()
