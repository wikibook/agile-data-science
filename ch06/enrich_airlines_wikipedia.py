import sys, os, re
sys.path.append("lib")
import utils

import wikipedia
from bs4 import BeautifulSoup
import tldextract

# 우리 항공사 목록 적재
our_airlines = utils.read_json_lines_file('data/our_airlines.jsonl')

# 위키피디아 데이터를 포함한 새로운 리스트 생성
with_url = []
for airline in our_airlines:
  # 항공사 이름으로 위키피디아 페이지 가져오기
  wikipage = wikipedia.page(airline['Name'])

  # 요약 정보(summary) 가져오기
  summary = wikipage.summary
  airline['summary'] = summary

  # 그 페이지의 HTML 가져오기
  page = BeautifulSoup(wikipage.html())

  # 작업: 오른쪽 'vcard' 열에서 로고 가져오기
  # 1) vcard 테이블 가져오기
  vcard_table = page.find_all('table', class_='vcard')[0]
  # 2) 로고는 언제나 이 테이블 안에 첫 번째 이미지임
  first_image = vcard_table.find_all('img')[0]
  # 3) 이미지에 대한 URL 설정
  logo_url = 'http:' + first_image.get('src')
  airline['logo_url'] = logo_url

  # 작업: 회사 웹사이트 가져오기
  # 1) 'Website' 테이블 헤더 찾기
  th = page.find_all('th', text='Website')[0]
  # 2) 부모 tr 요소 찾기
  tr = th.parent
  # 3) 해당 tr 내에서(링크) 태그 찾기
  a = tr.find_all('a')[0]
  # 4) 마지막으로, a 태그의 href를 가져오기
  url = a.get('href')
  airline['url'] = url

  # URL로 표시할 도메인 가져오기
  url_parts = tldextract.extract(url)
  airline['domain'] = url_parts.domain + '.' + url_parts.suffix

  with_url.append(airline)

utils.write_json_lines_file(with_url, 'data/our_airlines_with_wiki.jsonl')

