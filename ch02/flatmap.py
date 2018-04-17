csv_lines = sc.textFile("data/example.csv")

# 라인 단위로 단어의 관계 계산
words_by_line = csv_lines\
  .map(lambda line: line.split(","))

words_by_line.collect()

# 단어의 관계 계산
flattened_words = csv_lines\
  .map(lambda line: line.split(","))\
  .flatMap(lambda x: x)

flattened_words.collect()
