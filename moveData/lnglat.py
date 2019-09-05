#! -*- coding:utf-8 -*-


from con_oracle import *
from mapApi import *
import requests


def lng_lat():

    dba = Dba()
    con = dba.connect()
    cur = con.cursor()

    sql = "select detail_address, count(detail_address) as num from SDTRANSFER_SERVER  where  lon is null group by detail_address order by num desc"

    upsql = "update SDTRANSFER_SERVER set lon='%s', lat='%s' where detail_address='%s' "
    data = dba.query_data(sql)
    for d in data:
        if d[0] != 'None':
            address = "苏州市工业园区" + d[0]
        else:
            address = "苏州市工业园区"
        try:
            m = get_lng_lat(address)
            up = upsql % (m[0], m[1], d[0])
            cur.execute(up)    
            con.commit()
        except Exception as e:
            print(e) 
    cur.close()
    con.close()
