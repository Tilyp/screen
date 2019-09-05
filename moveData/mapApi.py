#! -*- coding: utf-8 -*-
import requests


sess = requests.session()

def get_lng_lat(address):
    try:
        url = "http://api.map.baidu.com/geocoder/v2/?address=%s&output=json&ak=ROUZ3zeB69pURmbyIISXGA55lL1DCghZ"
        req_url = url % address
        data = sess.get(req_url).json()
        if data['status'] == 1:
            return ("", "")       
        m = data["result"]["location"]
        return ("%.15f" % m['lng'], "%.15f" % m["lat"])
    except:
        print(address, "query result:", data)

if __name__ == "__main__":
    get_lng_lat("苏州市工业园区")
