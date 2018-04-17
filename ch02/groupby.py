csv_lines = sc.textFile("data/example.csv")

# CSV 줄을 객체로 전환
def csv_to_record(line):
  parts = line.split(",")
  record = {
    "name": parts[0],
    "company": parts[1],
    "title": parts[2]
  }
  return record

# 함수를 매 레코드마다 적용
records = csv_lines.map(csv_to_record)

# 데이터셋의 첫 번째 항목 검사
records.first()

# 사람 이름으로 레코드 그룹핑
grouped_records = records.groupBy(lambda x: x["name"])

# 첫 번째 그룹 보여주기
grouped_records.first()

# 그룹 수 세기
job_counts = grouped_records.map(
  lambda x: {
    "name": x[0],
    "job_count": len(x[1])
  }
)

job_counts.first()

job_counts.collect()
