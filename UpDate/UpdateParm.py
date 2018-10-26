# -*- coding:utf-8 -*-
import MySQLdb
import pandas as pd
from numpy import *
import numpy as np
import time,datetime
from dateutil.relativedelta import relativedelta
from solve import KMV
from sqlalchemy import create_engine
import datetime,time

class Cal():
    def __init__(self,secid,EndDate,engine):
        self.secid=secid
        self.EndDate=EndDate
        self.engine=engine

    def getRate(self):#获取报告日期的年月利率
        sql="SELECT R FROM interestrate WHERE reportdate='%s'"%self.EndDate
        cur=self.engine.execute(sql)
        results = cur.fetchone()
        return results[0]

    def getF(self):#获取报告日期的上市公司债务
        sql="SELECT totalliab FROM financial WHERE secid='%s' and reportdate='%s'"%(self.secid,self.EndDate)
        cur=self.engine.execute(sql)
        results = cur.fetchone()
        return results[0]

    def getSt(self):#获取报告日期的上市公司市值
        sql="SELECT ev FROM financial WHERE secid='%s' and reportdate='%s'"%(self.secid,self.EndDate)
        cur=self.engine.execute(sql)
        results = cur.fetchone()
        return results[0]


    def get_sigma_St(self,engine):#获取上市公司报告期前一年的股价收益波动率
        delta = relativedelta(years=-1,months=0,days=0)
        RangeStartDate=datetime.datetime.strptime(self.EndDate,'%Y%m%d')+delta #获取一年区间的起始日期
        sql="SELECT closeprice FROM stockprice WHERE secid='%s' and tradedate >='%s' and tradedate<='%s' and volume!=0"%(self.secid,RangeStartDate,self.EndDate)
        df=pd.read_sql(sql,con=engine)#将查询结果转化为dataframe格式
        data=df['closeprice'].tolist()#将dataframe转为list
        if data==[]:  #若返回为空集，则停牌了一年以上，没有数据
            return -1
        else:
            logreturns = diff(log(data))
            sigma_St= np.std(logreturns)*sqrt(250)
            return sigma_St
