#!/usr/bin/env bash

# 가상 운항에 대한 지연 예측 가져오기
curl -XPOST 'http://localhost:5000/flights/delays/predict/regress' \
  -F 'DepDelay=5.0' \
  -F 'Carrier=AA' \
  -F 'Date=2016-12-23' \
  -F 'Dest=ATL' \
  -F 'FlightNum=1519' \
  -F 'Origin=SFO' \
| json_pp
