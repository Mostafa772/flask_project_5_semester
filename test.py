import requests

BASE = "http://127.0.0.1:5000/"

response = requests.get(BASE + "avg_salary/ADM ENGINEER (NON MGRL)")
print(response.json())
