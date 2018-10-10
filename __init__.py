# -*- coding:utf-8 -*-
import MySQLdb
import pandas as pd
import numpy as np
import time,datetime
from DTD import KMV

def getCurrentTime():
    # 获取当前时间
    return time.strftime('%Y-%m-%d', time.localtime(time.time()))

def getAStock():
    cur.execute('SELECT secid FROM stocklist')
    results=cur.fetchall()
    return results

def getIpodate(secid):
    sql="SELECT ipodate FROM stocklist WHERE secid='%s'"%secid
    cur.execute(sql)
    results = cur.fetchone()
    ipodate=results[0]#获取股票上市日期
    return ipodate

#def get_date_list(StartDate,TodayDate):

class Stock():
    def __init__(self,secid,EndDate):
        self.secid=secid
        self.EndDate=EndDate

    def getRate(self):
        sql="SELECT R FROM interestrate WHERE reportdate='%s'"%EndDate
        cur.execute(sql)
        results = cur.fetchone()
        return results

    def getF(self):
        sql="SELECT totalliab FROM financial WHERE secid='%s' and reportdate='%s'"%(secid,EndDate)
        cur.execute(sql)
        results = cur.fetchone()
        return results

    def getVt(self):
        sql="SELECT totalassets FROM financial WHERE secid='%s' and reportdate='%s'"%(secid,EndDate)
        cur.execute(sql)
        results = cur.fetchone()
        return results

    def getSt(self):
        sql="SELECT closeprice FROM stockprice WHERE secid='%s' and tradedate='%s'"%(secid,EndDate)
        cur.execute(sql)
        results = cur.fetchone()
        return results

def main():
    #连接数据库
    global conn,cur,symbols,EndDate,secid
    conn = MySQLdb.connect(
        host='localhost',
        user='root',
        passwd='',
        db='testdb',
        )
    cur = conn.cursor()
    Stocklist=getAStock()
    for secid in Stocklist:
        secid=secid[0]
        StartDate=getIpodate(secid)
        TodayDate=getCurrentTime()
        Datelist=get_date_list(StartDate,TodayDate)
        for EndDate in Datelist:
            stock=Stock(secid,EndDate)
            EV=stock.getVt()
            F=stock.getF()
            r=stock.getRate()
            St=stock.getSt()
            DTD=KMV(EV,St,sigma_St,F,r)
    StartDATE=getIpodate('0000001.sz')

if __name__ == "__main__":
    main()


