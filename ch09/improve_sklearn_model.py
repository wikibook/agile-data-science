#
# 출발 지연 데이터 없이 도착 지연에 대해 동일한 예측을 시도 - 더 어려운 문제임
#

import sys, os, re
sys.path.append("lib")
import utils

import numpy as np
import sklearn
import iso8601
import datetime
print("Imports loaded...")

# 훈련 데이터 크기를 검사하고 적재. 1분 정도 소요.
print("Original JSON file size: {:,} Bytes".format(os.path.getsize("../data/simple_flight_delay_features.jsonl")))
training_data = utils.read_json_lines_file('../data/simple_flight_delay_features.jsonl')
print("Training items: {:,}".format(len(training_data))) # 5,714,008
print("Data loaded...")

# 데이터를 변경하기 전 레코드를 검사
print("Size of training data in RAM: {:,} Bytes".format(sys.getsizeof(training_data))) # 50MB
print(training_data[0])

# 먼저 훈련 데이터 표본 추출
# sampled_training_data = training_data#np.random.choice(training_data, 1000000)
# print("Sampled items: {:,} Bytes".format(len(training_data)))
# print("Data sampled...")

# 결과를 데이터 나머지 부분과 분리하고 벡터화한 다음 평가함 
results = [record['ArrDelay'] for record in training_data]
results_vector = np.array(results)
sys.getsizeof(results_vector) # 45,712,160 바이트
print("Results vectorized...")

# 훈련 데이터에서 출발/도착 지연 필드와 비행 날짜 데이터 제거
for item in training_data:
  item.pop('ArrDelay', None)
  item.pop('FlightDate', None)
  item.pop('DepDelay', None)
print("ArrDelay and FlightDate removed from training data...")

# 날짜/시간 문자열은 유닉스 시간으로 변환해야 함
for item in training_data:
  if isinstance(item['CRSArrTime'], str):
    dt = iso8601.parse_date(item['CRSArrTime'])
    unix_time = int(dt.timestamp())
    item['CRSArrTime'] = unix_time
  if isinstance(item['CRSDepTime'], str):
    dt = iso8601.parse_date(item['CRSDepTime'])
    unix_time = int(dt.timestamp())
    item['CRSDepTime'] = unix_time
print("Datetimes converted to unix times...")

# DictVectorizer를 사용해서 특징 dict를 벡터로 전환
from sklearn.feature_extraction import DictVectorizer

print("Original dimensions: [{:,}]".format(len(training_data)))
vectorizer = DictVectorizer()
training_vectors = vectorizer.fit_transform(training_data)
print("Size of DictVectorized vectors: {:,} Bytes".format(training_vectors.data.nbytes))
print("Training data vectorized...")

from sklearn.model_selection import train_test_split

X_train, X_test, y_train, y_test = train_test_split(
  training_vectors,
  results_vector,
  test_size=0.1,
  random_state=43
)
print(X_train.shape, X_test.shape)
print(y_train.shape, y_test.shape)
print("Test train split performed...")

#
# GradientBoostingRegressor를 위해 LinearRegression을 교환하고 특징 중요도를 결정함
#
from sklearn.ensemble import GradientBoostingRegressor

regressor = GradientBoostingRegressor
print("Swapped gradient boosting trees for linear regression!")

# 새 훈련 데이터에 회귀 모델을 다시 적합시킴
regressor.fit(X_train, y_train)
print("Regressor fitted again...")

# 다시 테스트 데이터를 사용해서 예측
predicted = regressor.predict(X_test)
print("Predictions made for X_test again...")

# http://scikit-learn.org/stable/modules/model_evaluation.html의 정의 참조
from sklearn.metrics import median_absolute_error, r2_score

# 문서에서 중위수 절대 오차(median absolute error)를 대상과 예측 값 간의 절대값 차이 전체 중 에 중앙값으로 정의한다.
# 이 값은 작은 것이 더 낫고 크다는 것은 대상과 예측 값 사이의 오차가 크다는 것을 의미한다.
medae = median_absolute_error(y_test, predicted)
print("Median absolute error:    {:.3g}".format(medae))

# R2 점수는 결정 계수 또는 미래 표본이 얼마나 잘 예측될 것인지에 대한 측정값이다. 
# 이 값은 1부터 0사이의 값을 가지며 1.0이 가장 좋고 0.0이 가장 나쁘다.
r2 = r2_score(y_test, predicted)
print("r2 score:                 {:.3g}".format(r2))

# 결과 그리지, 실제 값과 예측 값을 비교
import matplotlib.pyplot as plt

plt.scatter(
  y_test,
  predicted,
  color='blue',
  linewidth=1
)

plt.xticks(())
plt.yticks(())

plt.show()
