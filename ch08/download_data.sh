#!/usr/bin/env bash

# 이 스크립트의 절대 경로 가져오기, http://bit.ly/find_path 참조
ABSOLUTE_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/$(basename "${BASH_SOURCE[0]}")"
ABSOLUTE_DIR=$(dirname "${ABSOLUTE_PATH}")

# 우리가 어디에서 실행하든지 상관 없이 Agile_Data_Code_2/data/on_time_performance.parquet 추출
cd $ABSOLUTE_DIR/../data/
curl -Lko ./simple_flight_delay_features.jsonl.bz2 http://s3.amazonaws.com/agile_data_science/simple_flight_delay_features.jsonl.bz2

# 한 쌍의 공항 사이의 거리 가져오기
curl -Lko ./origin_dest_distances.jsonl http://s3.amazonaws.com/agile_data_science/origin_dest_distances.jsonl

# ch08/web/predict_flask.py가 동작할 수 있도록 모델 가져오기
cd $ABSOLUTE_DIR/..
mkdir models
curl -Lko ./models/sklearn_vectorizer.pkl http://s3.amazonaws.com/agile_data_science/sklearn_vectorizer.pkl
curl -Lko ./models/sklearn_regressor.pkl http://s3.amazonaws.com/agile_data_science/sklearn_regressor.pkl
