# -*- coding:utf-8 -*-
import MySQLdb
import pandas as pd
from numpy import *
import numpy as np
import time,datetime
from dateutil.relativedelta import relativedelta
from solve import KMV

def getCurrentTime():
    # 获取当前时间
    return time.strftime('%Y%m%d', time.localtime(time.time()))

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

def get_date_list(StartDate,TodayDate):#获取从上市日期到最新日期之间所有月末日期的序列
    DateList=pd.date_range(start=StartDate,end=TodayDate,freq='M')
    return DateList

class Stock():
    def __init__(self,secid,EndDate):
        self.secid=secid
        self.EndDate=EndDate

    def getRate(self):#获取报告日期的年月利率
        sql="SELECT R FROM interestrate WHERE reportdate='%s'"%EndDate
        cur.execute(sql)
        results = cur.fetchone()
        return results

    def getF(self):#获取报告日期的上市公司债务
        sql="SELECT totalliab FROM financial WHERE secid='%s' and reportdate='%s'"%(secid,EndDate)
        cur.execute(sql)
        results = cur.fetchone()
        return results

    def getVt(self):#获取报告日期的上市公司市值
        sql="SELECT totalassets FROM financial WHERE secid='%s' and reportdate='%s'"%(secid,EndDate)
        cur.execute(sql)
        results = cur.fetchone()
        return results

    def getSt(self):#获取报告日期的上市公司的股价
        delta = relativedelta(years=0,months=0,days=0)
        sql="SELECT closeprice FROM stockprice WHERE secid='%s' and tradedate='%s'"%(secid,EndDate)
        cur.execute(sql)
        results = cur.fetchone()
        return results

    def get_sigma_St(self,EndDate):#获取上市公司报告期前一年的股价收益波动率
        delta = relativedelta(years=-1,months=0,days=0)
        RangeStartDate=EndDate+delta #获取一年区间的起始日期
        sql="SELECT closeprice FROM stockprice WHERE secid='%s' and tradedate >='%s' and tradedate<='%s' and volume!=0"%(secid,RangeStartDate,EndDate)
        df=pd.read_sql(sql,conn)#将查询结果转化为dataframe格式
        data=df['closeprice'].tolist()#将dataframe转为list
        logreturns = diff(log(data))
        sigma_St= np.std(logreturns)
        return sigma_St

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
        secid='000001.SZ'
        StartDate=getIpodate(secid)
        TodayDate=getCurrentTime()
        Datelist=get_date_list(StartDate,TodayDate)
        for EndDate in Datelist:
            stock=Stock(secid,EndDate)
            EV=stock.getVt()
            F=stock.getF()
            r=stock.getRate()
            St=stock.getSt()
            sigma_St=stock.get_sigma_St(EndDate)
            DTD=KMV(EV,St,sigma_St,F,r)


if __name__ == "__main__":
    main()


