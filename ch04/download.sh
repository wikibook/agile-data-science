#
# 책에 쓰이는 데이터를 내려받기 위한 스크립트
#
mkdir ../data

# 2015년 모든 비행 데이터를 위한 정시 레코드 가져오기 - 273MB
wget -P ../data/ http://s3.amazonaws.com/agile_data_science/On_Time_On_Time_Performance_2015.csv.gz

# openflights 데이터 가져오기
wget -P /tmp/ https://raw.githubusercontent.com/jpatokal/openflights/master/data/airports.dat
mv /tmp/airports.dat ../data/airports.csv

wget -P /tmp/ https://raw.githubusercontent.com/jpatokal/openflights/master/data/airlines.dat
mv /tmp/airlines.dat ../data/airlines.csv

wget -P /tmp/ https://raw.githubusercontent.com/jpatokal/openflights/master/data/routes.dat
mv /tmp/routes.dat ../data/routes.csv

wget -P /tmp/ https://raw.githubusercontent.com/jpatokal/openflights/master/data/countries.dat
mv /tmp/countries.dat ../data/countries.csv

# FAA data 가져오기
wget -P ../data/ http://av-info.faa.gov/data/ACRef/tab/aircraft.txt
wget -P ../data/ http://av-info.faa.gov/data/ACRef/tab/ata.txt
wget -P ../data/ http://av-info.faa.gov/data/ACRef/tab/compt.txt
wget -P ../data/ http://av-info.faa.gov/data/ACRef/tab/engine.txt
wget -P ../data/ http://av-info.faa.gov/data/ACRef/tab/prop.txt

# Aircraft 데이터베이스 가져오기
# wget -P /tmp/ http://registry.faa.gov/database/AR042016.zip
# unzip -d ../data/ /tmp/AR042016.zip

# FAA 등록 data 가져오기
# wget -P /tmp/ http://registry.faa.gov/database/AR042016.zip
# unzip -d ../data/ /tmp/AR042016.zip
