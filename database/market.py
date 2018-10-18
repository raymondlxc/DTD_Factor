# -*- coding:utf-8 -*-
# Author:OpenAPISupport@wind.com.cn
# Editdate:2018-10-04

from WindPy import *
import MySQLdb
import datetime

w.start()

#连接数据库
conn = MySQLdb.connect(
    host='localhost',
    user='root',
    passwd='',
    db='testdb',
    )
cur = conn.cursor()

#创建数据库表
cur.execute("DROP TABLE IF EXISTS stockprice")
cur.execute("""
CREATE TABLE stockprice(
    secid VARCHAR(20) NOT NULL,
    tradedate VARCHAR(20),
    openprice FLOAT,
    highprice FLOAT,
    lowprice FLOAT,
    closeprice FLOAT,
    volume FLOAT,
    amt FLOAT,
    turn FLOAT)""");
#secid:股票代码；tradedate交易日期;openprice开盘价;highprice最高价;lowprice最低价;closeprice收盘价;volume成交量;amt成交额;turn换手率
#通过wset提取板块成分数据集
print('\n\n'+'-----通过wset来取数据集数据,获取广东省上市公司代码列表-----'+'\n')
todayDate=datetime.datetime.strftime(datetime.date.today(),"%Y%m%d")
wsetdata=w.wset('SectorConstituent','date='+todayDate+';sectorId=0304050000000000;field=wind_code')
print(wsetdata)

for j in range(len(wsetdata.Data[0])):
    #通过wsd提取沪深300高开低收数据
    print("\n\n-----第 %i 次通过wsd来提取 %s 开高低收成交量数据-----\n" %(j,str(wsetdata.Data[0][j])))

    wssdata=w.wss(str(wsetdata.Data[0][j]),'ipo_date')
    wsddata=w.wsd(str(wsetdata.Data[0][j]), "open,high,low,close,volume,amt,turn", wssdata.Data[0][0], todayDate, "Fill=Previous")

    if wsddata.ErrorCode!=0:
        continue
    print(wsddata)
    for i in range(len(wsddata.Data[0])):
        sqllist=[]
        sqltuple=()
        sqllist.append(str(wsetdata.Data[0][j]))
        if len(wsddata.Times)>1:
            sqllist.append(wsddata.Times[i].strftime('%Y%m%d'))
        for k in range(len(wsddata.Fields)):
            sqllist.append(wsddata.Data[k][i])
        sqltuple=tuple(sqllist)
        cur.execute("INSERT INTO stockprice VALUES('%s','%s','%f','%f','%f','%f','%f','%f','%f')"%sqltuple)
    conn.commit()
conn.close()



