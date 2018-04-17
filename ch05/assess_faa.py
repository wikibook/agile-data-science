# FAA N-Number 검색 레코드 적재
faa_tail_number_inquiry = spark.read.json('data/faa_tail_number_inquiry.jsonl')
faa_tail_number_inquiry.show()

# 레코드 개수 세기
faa_tail_number_inquiry.count()

# 고유한 꼬리 번호 적재
unique_tail_numbers = spark.read.json('data/tail_numbers.jsonl')
unique_tail_numbers.show()

# 꼬리 번호와 검색 결과를 왼쪽 외부 조인해 얼마나 많이 일치하는지 확인
tail_num_plus_inquiry = unique_tail_numbers.join(
  faa_tail_number_inquiry,
  unique_tail_numbers.TailNum == faa_tail_number_inquiry.TailNum,
  'left_outer'
)
tail_num_plus_inquiry.show()

# 전체 레코드 수와 성공적으로 조인된 레코드 수를 계산
total_records = tail_num_plus_inquiry.count()
join_hits = tail_num_plus_inquiry.filter(
  tail_num_plus_inquiry.owner.isNotNull()
).count()

# 파이썬으로 조인율을 계산해 출력
hit_ratio = float(join_hits)/float(total_records)
hit_pct = hit_ratio * 100
print("Successful joins: {:.2f}%".format(hit_pct))
