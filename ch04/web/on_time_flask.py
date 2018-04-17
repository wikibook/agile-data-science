from flask import Flask, render_template, request
from pymongo import MongoClient
from bson import json_util

# 플라스크와 몽고 설정
app = Flask(__name__)
client = MongoClient()

# 컨트롤러: 이메일을 가져와서 디스플레이
@app.route("/on_time_performance")
def on_time_performance():
  
  carrier = request.args.get('Carrier')
  flight_date = request.args.get('FlightDate')
  flight_num = request.args.get('FlightNum')
  
  flight = client.agile_data_science.on_time_performance.find_one({
    'Carrier': carrier,
    'FlightDate': flight_date,
    'FlightNum': int(flight_num)
  })
  
  return json_util.dumps(flight)

if __name__ == "__main__":
  app.run(debug=True)
