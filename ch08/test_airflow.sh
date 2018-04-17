#!/bin/bash

# 오늘 날짜 계산:
export ISO_DATE=`date "+%Y-%m-%d"`

# DAG 목록 확인
airflow list_dags

# 각 DAG의 작업 목록 확인
airflow list_tasks agile_data_science_batch_prediction_model_training
airflow list_tasks agile_data_science_batch_predictions_daily

# 각 DAG의 각 작업 테스트
airflow test agile_data_science_batch_prediction_model_training pyspark_extract_features $ISO_DATE
airflow test agile_data_science_batch_prediction_model_training pyspark_train_classifier_model $ISO_DATE

airflow test agile_data_science_batch_predictions_daily pyspark_fetch_prediction_requests $ISO_DATE
airflow test agile_data_science_batch_predictions_daily pyspark_make_predictions $ISO_DATE
airflow test agile_data_science_batch_predictions_daily pyspark_load_prediction_results $ISO_DATE

# 모델 훈련과 유지 결과 테스트 
airflow backfill -s $ISO_DATE -e $ISO_DATE agile_data_science_batch_prediction_model_training

# 모델의 일별 운영 결과 테스트
airflow backfill -s $ISO_DATE -e $ISO_DATE agile_data_science_batch_predictions_daily
