import requests


sess = requests.session()

url = "http://172.25.235.17:5000/order_total"

data = {"beginTime": "2019-01-26 00:00:00", "endTime": "2019-01-27 00:00:00"}

res = sess.post(url, data).json()
print(res)
