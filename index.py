#! -*- coding: utf-8 -*-
from flask import Flask
from flask_restful import Api, Resource, reqparse
from db.moveCar import *
from zteEbaseAccess import call_api
app = Flask(__name__)
api = Api(app)

# @app.route('/')
# def index():
#    return 'Hello Flask'

fromat = {
  "data": {},
  "code": '200',
  "message": '',
  "success": ''
}

class CaseAccordCount(Resource):
    def __init__(self):
        self.parser = reqparse.RequestParser()
        self.parser.add_argument('beginTime', type=str, help='begin need string')
        self.parser.add_argument('endTime', type=str, help='endTime need string')

    def post(self):
        args = self.parser.parse_args()
        beginTime = args["beginTime"]        
        endTime = args["endTime"]
        data = caseAccordCount(beginTime, endTime)
        result = fromat.copy()
        result["data"] = data
        return result
            
class CountCard(Resource):
    def __init__(self):
        self.parser = reqparse.RequestParser()
        self.parser.add_argument('beginTime', type=str, help='begin need string')
        self.parser.add_argument('endTime', type=str, help='endTime need string')
    def post(self):
        args = self.parser.parse_args()
        beginTime = args["beginTime"]
        endTime = args["endTime"]
        data = countCard(beginTime, endTime)
        result = fromat.copy()
        result["data"] = data
        return result

class Road(Resource):
    def __init__(self):
        self.parser = reqparse.RequestParser()
        self.parser.add_argument('beginTime', type=str, help='begin need string')
        self.parser.add_argument('endTime', type=str, help='endTime need string')
    def post(self):
        args = self.parser.parse_args()
        beginTime = args["beginTime"]
        endTime = args["endTime"]
        data = road(beginTime, endTime, '010999')
        result = fromat.copy()
        result["data"] = data
        return result

class Place(Resource):
    def __init__(self):
        self.parser = reqparse.RequestParser()
        self.parser.add_argument('beginTime', type=str, help='begin need string')
        self.parser.add_argument('endTime', type=str, help='endTime need string')
    def post(self):
        args = self.parser.parse_args()
        beginTime = args["beginTime"]
        endTime = args["endTime"]
        data = road(beginTime, endTime, '010907')
        result = fromat.copy()
        result["data"] = data
        return result

class Village(Resource):
    def __init__(self):
        self.parser = reqparse.RequestParser()
        self.parser.add_argument('beginTime', type=str, help='begin need string')
        self.parser.add_argument('endTime', type=str, help='endTime need string')
    def post(self):
        args = self.parser.parse_args()
        beginTime = args["beginTime"]
        endTime = args["endTime"]
        data = road(beginTime, endTime, '030806')
        result = fromat.copy()
        result["data"] = data
        return result

class Focus(Resource):
    def __init__(self):
        self.parser = reqparse.RequestParser()
        self.parser.add_argument('beginTime', type=str, help='begin need string')
        self.parser.add_argument('endTime', type=str, help='endTime need string')
    def post(self):
        args = self.parser.parse_args()
        beginTime = args["beginTime"]
        endTime = args["endTime"]
        data = focus(beginTime, endTime)
        result = fromat.copy()
        result["data"] = data
        return result

class LngLat(Resource):
    def __init__(self):
        self.parser = reqparse.RequestParser()
        self.parser.add_argument('beginTime', type=str, help='begin need string')
        self.parser.add_argument('endTime', type=str, help='endTime need string')
    def post(self):
        args = self.parser.parse_args()
        beginTime = args["beginTime"]
        endTime = args["endTime"]
        data = lng_lat(beginTime, endTime)
        result = fromat.copy()
        result["data"] = data
        return result 

class OrderTotal(Resource):
    def __init__(self):
        self.parser = reqparse.RequestParser()
        self.parser.add_argument('beginTime', type=str, help='begin need string')
        self.parser.add_argument('endTime', type=str, help='endTime need string')
    def post(self):
        args = self.parser.parse_args()
        beginTime = args["beginTime"]
        endTime = args["endTime"]
        data = order_total(beginTime, endTime)
        queue = queue_total(beginTime, endTime)
        out = out_total(beginTime, endTime) 
        status = call_api()
        data["zteStatus"] = status
        data["detailMaster"] = queue["detail"]
        data["queueDetail"] = queue["queue"]
        data["outPhone"] = out["out"]
        result = fromat.copy()
        result["data"] = data
        return result

api.add_resource(CaseAccordCount, "/caseAccordCount")
api.add_resource(CountCard, "/countCard")
api.add_resource(Place, "/road")
api.add_resource(Village, "/village")
api.add_resource(Road, "/public_place")
api.add_resource(Focus, "/focus")
api.add_resource(LngLat, "/lnglat")
api.add_resource(OrderTotal, "/order_total")

if __name__ == "__main__":
    app.run(host="172.25.235.17", port=5000, debug=True)

