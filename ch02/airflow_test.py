import sys, os, re

from airflow import DAG
from airflow.operators.bash_operator import BashOperator

from datetime import datetime, timedelta
import iso8601

project_home = os.environ["PROJECT_HOME"]

default_args = {
  'owner': 'airflow',
  'depends_on_past': False,
  'start_date': iso8601.parse_date("2016-12-01"),
  'email': ['russell.jurney@gmail.com'],
  'email_on_failure': True,
  'email_on_retry': True,
  'retries': 3,
  'retry_delay': timedelta(minutes=5),
}

# Timedelta 1은 '일 별 실행'을 의미
dag = DAG(
  'agile_data_science_airflow_test',
  default_args=default_args,
  schedule_interval=timedelta(1)
)

# 간단한 PySpark 스크립트 실행
pyspark_local_task_one = BashOperator(
  task_id = "pyspark_local_task_one",
  bash_command = """spark-submit \
  --master {{ params.master }}
  {{ params.base_path }}/{{ params.filename }} {{ ts }} {{ params.base_path }}""",
  params = {
    "master": "local[8]",
    "filename": "ch02/pyspark_task_one.py",
    "base_path": "{}/".format(project_home)
  },
  dag=dag
)

# 이전 PySpark 스크립트에 의존한 또다른 PySpark 스크립트 실행
pyspark_local_task_two = BashOperator(
  task_id = "pyspark_local_task_two",
  bash_command = """spark-submit \
  --master {{ params.master }}
  {{ params.base_path }}/{{ params.filename }} {{ ts }} {{ params.base_path }}""",
  params = {
    "master": "local[8]",
    "filename": "ch02/pyspark_task_two.py",
    "base_path": "{}/".format(project_home)
  },
  dag=dag
)

# 두 번째 작업에 첫 번째 작업에 대한 종속성 추가  
pyspark_local_task_two.set_upstream(pyspark_local_task_one)
