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
        return results[0]

    def getF(self):#获取报告日期的上市公司债务
        sql="SELECT totalliab FROM financial WHERE secid='%s' and reportdate='%s'"%(secid,EndDate)
        cur.execute(sql)
        results = cur.fetchone()
        return results[0]

    def getSt(self):#获取报告日期的上市公司市值
        sql="SELECT totalassets FROM financial WHERE secid='%s' and reportdate='%s'"%(secid,EndDate)
        cur.execute(sql)
        results = cur.fetchone()
        return results[0]


    def get_sigma_St(self):#获取上市公司报告期前一年的股价收益波动率
        delta = relativedelta(years=-1,months=0,days=0)
        RangeStartDate=datetime.datetime.strptime(EndDate,'%Y%m%d')+delta #获取一年区间的起始日期
        sql="SELECT closeprice FROM stockprice WHERE secid='%s' and tradedate >='%s' and tradedate<='%s' and volume!=0"%(secid,RangeStartDate,EndDate)
        df=pd.read_sql(sql,conn)#将查询结果转化为dataframe格式
        data=df['closeprice'].tolist()#将dataframe转为list
        if data==[]:  #若返回为空集，则停牌了一年以上，没有数据
            return -1
        else:
            logreturns = diff(log(data))
            sigma_St= np.std(logreturns)*sqrt(250)
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
    #创建数据库表
    cur.execute("DROP TABLE IF EXISTS Results")
    cur.execute("""
        CREATE TABLE Results(
        secid VARCHAR(20) NOT NULL,
        EndDate VARCHAR(20),
        St FLOAT,
        sigma_St FLOAT,
        F FLOAT,
        Vt_ini FLOAT,
        r FLOAT,
        Vt FLOAT,
        sigma FLOAT,
        dt FLOAT,
        status VARCHAR(20)
        )""");
    Stocklist=getAStock()
    for secid in Stocklist:
        secid=secid[0] #读取元组中的数据
        StartDate=getIpodate(secid)
        if StartDate<'20020131':#数据有效日期从2002年1月31日开始
            StartDate='20020131'
        #TodayDate=getCurrentTime()
        #更新数据时使用
        TodayDate='20180930'
        Datelist=get_date_list(StartDate,TodayDate)
        for EndDate in Datelist:
            EndDate=EndDate.strftime('%Y%m%d')#Datelist中生成的日期序列为日期格式，将其转为字符串便于数据库检索
            stock=Stock(secid,EndDate)
            St=stock.getSt()/1000000000
            sigma_St=stock.get_sigma_St()
            if sigma_St==-1: #若股票停牌一年以上，则退出该次循环
                continue
            F=stock.getF()/1000000000
            Vt_ini=F+St#资产价值为账面市值加负债，作为方程的初值求解
            r=stock.getRate()/100
            try:
                DTD=KMV(Vt_ini,St,sigma_St,F,r)
            except ValueError:
                status='fail'#错误类型为传入参数初值有误
                print(secid+'日期'+EndDate+'DTD计算失败')
                sqltuple=(secid,EndDate,St,sigma_St,F,Vt_ini,r,0,0,0,status)
                cur.execute("INSERT INTO Results VALUES('%s','%s','%f','%f','%f','%f','%f','%f','%f','%f','%s')"%sqltuple)
            except Exception:
                status='Runtime'#以状态字判断迭代超时
                print('正在计算'+secid+'日期'+EndDate+'DTD:迭代超时')
                print(DTD)
                Vt=DTD[0]
                sigma=DTD[1]
                dt=DTD[2]
                sqltuple=(secid,EndDate,St,sigma_St,F,Vt_ini,r,Vt,sigma,dt,status)
                cur.execute("INSERT INTO Results VALUES('%s','%s','%f','%f','%f','%f','%f','%f','%f','%f','%s')"%sqltuple)
            else:
                status='success'#以状态字判断迭代成功
                print('正在计算'+secid+'日期'+EndDate+'DTD:成功')
                print(DTD)
                Vt=DTD[0]
                sigma=DTD[1]
                dt=DTD[2]
                sqltuple=(secid,EndDate,St,sigma_St,F,Vt_ini,r,Vt,sigma,dt,status)
                cur.execute("INSERT INTO Results VALUES('%s','%s','%f','%f','%f','%f','%f','%f','%f','%f','%s')"%sqltuple)
            conn.commit()
    conn.close()

if __name__ == "__main__":
    main()


