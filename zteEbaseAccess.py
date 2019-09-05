#! -*- coding: utf-8 -*-

import json
from suds.client import Client
from bs4 import BeautifulSoup

url="http://172.23.0.41:16501/EbaseAccess2/webservice/ZteEbaseAccess?wsdl"
headers = {'Content-Type': 'application/soap+xml; charset="UTF-8"'}
client = Client(url,headers=headers,faults=False,timeout=15)
agent = ["14512167000" + str(i) for i in range(1, 10)] + ["1451216700" + str(i) for i in range(10, 52)]

def call_api():
    Sqlid="ebase.query_cc_moperstatus"
    Variables= "<variables><variable1>agentid,starttime, mainstatus, substatus, agentphone,ipaddress</variable1><variable2>400</variable2></variables>"
    result = client.service.queryBySqlRequest(key="", vcid='400',
     sqlid=Sqlid, variables=Variables, reserve1="", reserve2="",
     reserve3="", reserve4="", reserve5="")
    data = str(result[1]).split("ultinfo = ")[1].split("reserve1")[0]
    soup = BeautifulSoup(data.strip().strip('"'), 'lxml')
    records = soup.find_all("record")
    status = []
    for record in records:
        substatus = record.find("substatus").text
        agentphone = record.find("agentphone").text
        starttime = record.find("starttime").text
        mainstatus = record.find("mainstatus").text
        ipaddress = record.find("ipaddress").text
        agentid = record.find("agentid").text
        if agentphone in agent:
            # print(substatus, agentphone, mainstatus, ipaddress, agentid, starttime)
            status.append(mainstatus)
    return status

if __name__ == "__main__":
    call_api()
