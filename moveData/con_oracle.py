#! -*- coding:utf-8 -*-
import os

os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'

import cx_Oracle as co

import sys
reload(sys)
sys.setdefaultencoding('utf8')
class CarDba(object):

    def __init__(self):
        pass

    def connect(self):
        tns = co.makedsn('172.25.235.3', 1521, 'orcl')
        db = co.connect('dsjfx', 'dsjfx', tns)
        return db

    def cursor(self):
        consor = self.connect().cursor()
        return consor

    def query_data(self, sql):
        cursor = self.cursor()
        cursor.execute(sql)
        data = cursor.fetchall()
        return data

class PhoneDba(object):

    def __init__(self):
        pass

    def connect(self):
        tns = co.makedsn('172.25.205.3', 1521, 'szyq12345')
        db = co.connect('yqdj', 'yqdj', tns)
        return db

    def cursor(self):
        consor = self.connect().cursor()
        return consor

    def query_data(self, sql):
        cursor = self.cursor()
        cursor.execute(sql)
        data = cursor.fetchall()
        return data

class Dba(object):

    def __init__(self):
        pass

    def connect(self):
        tns = co.makedsn('172.25.235.15', 1521, 'orcl')
        db = co.connect('szyq', '123456', tns)
        return db

    def cursor(self):
        consor = self.connect().cursor()
        return consor

    def query_data(self, sql):
        cursor = self.cursor()
        cursor.execute(sql)
        data = cursor.fetchall()
        return data

    def cux_sql(self, sql):
        cursor = self.cursor()
        cursor.execute(sql)


    def close(self):
        cur = self.cursor()
        db = self.connect()
        db.commit()
        cur.close()
        db.close()
