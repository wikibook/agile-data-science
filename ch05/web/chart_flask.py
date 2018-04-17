from flask import Flask, render_template, request
from pymongo import MongoClient
from bson import json_util

# 플라스크와 몽고 설정
app = Flask(__name__)
client = MongoClient()

# 5장 컨트롤러: 비행 데이터 가져와서 디스플레이함
@app.route("/on_time_performance")
def on_time_performance():
  
  carrier = request.args.get('Carrier')
  flight_date = request.args.get('FlightDate')
  flight_num = request.args.get('FlightNum')
  
  flight = client.agile_data_science.on_time_performance.find_one({
    'Carrier': carrier,
    'FlightDate': flight_date,
    'FlightNum': flight_num
  })
  
  return render_template('flight.html', flight=flight)

# 5장 컨트롤러: 해당 일자에 두 도시 간 모든 비행 데이터를 가져와서 디스플레이함
@app.route("/flights/<origin>/<dest>/<flight_date>")
def list_flights(origin, dest, flight_date):
  
  flights = client.agile_data_science.on_time_performance.find(
    {
      'Origin': origin,
      'Dest': dest,
      'FlightDate': flight_date
    },
    sort = [
      ('DepTime', 1),
      ('ArrTime', 1),
    ]
  )
  flight_count = flights.count()
  
  return render_template('flights.html', flights=flights, flight_date=flight_date, flight_count=flight_count)

# 컨트롤러: 비행 테이블 가져오기
@app.route("/total_flights")
def total_flights():
  total_flights = client.agile_data_science.flights_by_month.find({}, 
    sort = [
      ('Year', 1),
      ('Month', 1)
    ])
  return render_template('total_flights.html', total_flights=total_flights)

# 비동기 요청(구 'AJAX')을 통해 차트용 데이터를 제공
@app.route("/total_flights.json")
def total_flights_json():
  total_flights = client.agile_data_science.flights_by_month.find({}, 
    sort = [
      ('Year', 1),
      ('Month', 1)
    ])
  return json_util.dumps(total_flights, ensure_ascii=False)

# 컨트롤러: 비행 차트 가져오기
@app.route("/total_flights_chart")
def total_flights_chart():
  total_flights = client.agile_data_science.flights_by_month.find({}, 
    sort = [
      ('Year', 1),
      ('Month', 1)
    ])
  return render_template('total_flights_chart.html', total_flights=total_flights)

# 컨트롤러: 비행 데이터 가져와서 디스플레이함
@app.route("/airplane/flights/<tail_number>")
def flights_per_airplane(tail_number):
  flights = client.agile_data_science.flights_per_airplane.find_one({'TailNum': tail_number})
  return render_template('flights_per_airplane.html', flights=flights, tail_number=tail_number)

if __name__ == "__main__":
  app.run(debug=True)
