#!/bin/bash

# 우리의 보강된 항공사 데이터를 'airlines' 컬렉션으로 임포트
mongoimport -d agile_data_science -c origin_dest_distances --file data/origin_dest_distances.jsonl
mongo agile_data_science --eval 'db.origin_dest_distances.ensureIndex({Origin: 1, Dest: 1})'
