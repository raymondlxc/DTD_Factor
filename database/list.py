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
    charset="utf8",
    )
cur = conn.cursor()
cur.execute("DROP TABLE IF EXISTS stocklist")
#创建数据库表
cur.execute("""
CREATE TABLE stocklist(
    secid VARCHAR(20) NOT NULL,
    secname VARCHAR(50),
    ipodate VARCHAR(50),
    city VARCHAR(50))
    DEFAULT CHARSET=utf8
""");

#通过wset提取板块成分数据集
print('\n\n'+'-----通过wset来取数据集数据,获取广东上市公司代码列表-----'+'\n')
todayDate='20181004'
#todayDate=datetime.datetime.strftime(datetime.date.today(),"%Y%m%d")
wsetdata=w.wset('SectorConstituent','date='+todayDate+';sectorId=0304050000000000;field=wind_code')
print(wsetdata)

for j in range(len(wsetdata.Data[0])):
    #通过wsd提取广东上市公司基本资料

    wssdata=w.wss(str(wsetdata.Data[0][j]),'sec_name,ipo_date,exch_city')
    sqllist=[]
    sqltuple=()
    sqllist.append(str(wsetdata.Data[0][j]))
    for k in range(len(wssdata.Fields)):
        sqllist.append(str(wssdata.Data[k][0]))
    sqltuple=tuple(sqllist)
    print(sqltuple)
    cur.execute("insert into stocklist VALUES('%s','%s','%s','%s')"%sqltuple)
    conn.commit()
conn.close()



