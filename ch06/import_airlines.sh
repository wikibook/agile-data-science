#!/bin/bash

# 우리가 보강했던 항공사 데이터를 'airlines' 컬렉션으로 임포트함 
mongoimport -d agile_data_science -c airlines --file data/our_airlines_with_wiki.jsonl
