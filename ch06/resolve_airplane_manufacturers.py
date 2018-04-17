airplanes = spark.read.json('data/airplanes.json')

airplanes.registerTempTable("airplanes")
manufacturer_variety = spark.sql(
"""SELECT
  DISTINCT(Manufacturer) AS Manufacturer
FROM
  airplanes
ORDER BY
  Manufacturer"""
)
manufacturer_variety_local = manufacturer_variety.collect()

# 이 레코드를 왼쪽 맞춤으로 출력해야 함
for mfr in manufacturer_variety_local:
  print(mfr.Manufacturer)

# 한 쌍의 문자열에서 앞에서부터 가장 긴 공통 문자열을 탐지
def longest_common_beginning(s1, s2):
  if s1 == s2:
    return s1
  min_length = min(len(s1), len(s2))
  i = 0
  while i < min_length:
    if s1[i] == s2[i]:
      i += 1
    else:
      break
  return s1[0:i]

# 두 제조사를 비교, 결과를 기술한 튜플을 반환
def compare_manufacturers(mfrs):
  mfr1 = mfrs[0]
  mfr2 = mfrs[1]
  lcb = longest_common_beginning(mfr1, mfr2)
  lcb = lcb.strip() # remove extra spaces
  len_lcb = len(lcb)
  record = {
    'mfr1': mfr1,
    'mfr2': mfr2,
    'lcb': lcb,
    'len_lcb': len_lcb,
    'eq': mfr1 == mfr2
  }
  return record

# 비교를 위해 모든 Manufacturer 필드의 고유 인스턴스를 그 외 나머지 모두와 쌍을 지음
comparison_pairs = manufacturer_variety.join(manufacturer_variety)

# 비교를 수행
comparisons = comparison_pairs.rdd.map(compare_manufacturers)

# 앞에서부터 5개 이상의 문자가 공통되면 matches에 공통 문자열을 넣음
matches = comparisons.filter(lambda f: f['eq'] == False and f['len_lcb'] > 5)

#
# 이제 중복 키를 원래 값에서 우리가 사용할 값으로 매핑을 만듦
#

# 1) matches를 앞에서부터 가장 긴 공통 문자('lcb')로 그룹핑
common_lcbs = matches.groupBy(lambda x: x['lcb'])

# 2) 일치하는 각 값에 키 값 'lcb'와 함께 원시 값을 내보냄
mfr1_map = common_lcbs.map(lambda x: [(y['mfr1'], x[0]) for y in x[1]]).flatMap(lambda x: x)
mfr2_map = common_lcbs.map(lambda x: [(y['mfr2'], x[0]) for y in x[1]]).flatMap(lambda x: x)

# 3) 비교한 두 레코드를 결합
map_with_dupes = mfr1_map.union(mfr2_map)

# 4) 중복 제거
mfr_dedupe_mapping = map_with_dupes.distinct()

# 5) 항공기 dataframe에 조인하기 위해 매핑을 dataframe으로 전환
mapping_dataframe = mfr_dedupe_mapping.toDF()

# 6) 매핑 열 이름 제공
mapping_dataframe.registerTempTable("mapping_dataframe")
mapping_dataframe = spark.sql(
  "SELECT _1 AS Raw, _2 AS NewManufacturer FROM mapping_dataframe"
)

# 매핑 테이블을 왼쪽 외부 조인(left outer join)함...
airplanes_w_mapping = airplanes.join(
  mapping_dataframe,
  on=airplanes.Manufacturer == mapping_dataframe.Raw,
  how='left_outer'
)
# 필요에 따라 Manufacturer를 NewManufacturer로 대체
airplanes_w_mapping.registerTempTable("airplanes_w_mapping")
resolved_airplanes = spark.sql("""SELECT
  TailNum,
  SerialNumber,
  Owner,
  OwnerState,
  IF(NewManufacturer IS NOT null,NewManufacturer,Manufacturer) AS Manufacturer,
  Model,
  ManufacturerYear,
  EngineManufacturer,
  EngineModel
FROM
  airplanes_w_mapping""")

# 나중에 사용할 수 있도록 airplanes.json 위치에 저장
resolved_airplanes.repartition(1).write.mode("overwrite").json("data/resolved_airplanes.json")
