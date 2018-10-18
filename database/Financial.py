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
cur.execute("DROP TABLE IF EXISTS financial")
cur.execute("""
CREATE TABLE financial(
    secid VARCHAR(20) NOT NULL,
    reportdate VARCHAR(20),
    totalassets FLOAT,
    totalliab FLOAT,
    netprofit FLOAT,
    ev FLOAT,
    totalshares FLOAT,
    operev FLOAT,
    eps FLOAT)""");
#secid:股票代码；reportdate:月末报告期；totalassets:总资产;totalliab:总负债;netprofit:净利润;ev:总市值;totalshares:总股本;operev:营业总收入;EPS:每股收益
#通过wset提取板块成分数据集
print('\n\n'+'-----通过wset来取数据集数据,获取广东省上市公司代码列表-----'+'\n')
todayDate=datetime.datetime.strftime(datetime.date.today(),"%Y%m%d")
wsetdata=w.wset('SectorConstituent','date='+todayDate+';sectorId=0304050000000000;field=wind_code')
print(wsetdata)

for j in range(len(wsetdata.Data[0])):
    #通过wsd提取沪深300高开低收数据
    print("\n\n-----第 %i 次通过wsd来提取 %s 开高低收成交量数据-----\n" %(j,str(wsetdata.Data[0][j])))

    wssdata=w.wss(str(wsetdata.Data[0][j]),'ipo_date')
    wsddata=w.wsd(str(wsetdata.Data[0][j]), "tot_assets,tot_liab,net_profit_is,ev,total_shares,tot_oper_rev,eps_basic", wssdata.Data[0][0], todayDate, "unit=1;rptType=1;currencyType=;Period=M;Days=Alldays;ShowBlank=-1")
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
        cur.execute("INSERT INTO financial VALUES('%s','%s','%f','%f','%f','%f','%f','%f','%f')"%sqltuple)
    conn.commit()
conn.close()



