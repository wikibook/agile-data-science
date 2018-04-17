#!/usr/bin/env python
#
# 파이썬을 사용해서 JSON과 JSON Lines 파일을 읽고 쓰기
#
import sys, os, re
import json
import codecs

ary_of_objects = [
  {'name': 'Russell Jurney', 'title': 'CEO'},
  {'name': 'Muhammad Imran', 'title': 'VP of Marketing'},
  {'name': 'Fe Mata', 'title': 'Chief Marketing Officer'},
]

path = "/tmp/test.jsonl"

#
# 객체를 jsonl에 쓰기
#
f = codecs.open(path, 'w', 'utf-8')
for row_object in ary_of_objects:
  # ensure_ascii=False is essential or errors/corruption will occur
  json_record = json.dumps(row_object, ensure_ascii=False)
  f.write(json_record + "\n")
f.close()

print("Wrote JSON Lines file /tmp/test.jsonl")

#
# 이 jsonl을 다시 객체로 읽어 들이기
#
ary_of_objects = []
f = codecs.open(path, "r", "utf-8")
for line in f:
  record = json.loads(line.rstrip("\n|\r"))
  ary_of_objects.append(record)
print(ary_of_objects)
print("Read JSON Lines file /tmp/test.jsonl")

#
# json과 jsonl 파일을 읽고 쓰기 위한 유틸리티 함수
#
def write_json_file(obj, path):
  '''객체를 덤프해서 파일에 json으로 씀'''
  f = codecs.open(path, 'w', 'utf-8')
  f.write(json.dumps(obj, ensure_ascii=False))
  f.close()

def write_json_lines_file(ary_of_objects, path):
  '''객체 리스트를 json lines 파일로 덤프함'''
  f = codecs.open(path, 'w', 'utf-8')
  for row_object in ary_of_objects:
    json_record = json.dumps(row_object, ensure_ascii=False)
    f.write(json_record + "\n")
  f.close()

def read_json_file(path):
  '''일반 json 파일(레코드 당 CR 없이)을 객체로 변환'''
  text = codecs.open(path, 'r', 'utf-8').read()
  return json.loads(text)

def read_json_lines_file(path):
  '''json lines 파일(레코드 당 CR 존재)을 객체 배열로 변환'''
  ary = []
  f = codecs.open(path, "r", "utf-8")
  for line in f:
    record = json.loads(line.rstrip("\n|\r"))
  return ary
