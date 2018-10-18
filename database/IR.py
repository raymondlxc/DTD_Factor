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
cur.execute("DROP TABLE IF EXISTS interestrate")
cur.execute("""
CREATE TABLE interestrate(
    reportdate VARCHAR(20) NOT NULL,
    R FLOAT)
""");
#reportdate:每月月末报告一次；R:一年定期存款（整存整取）月度年利率，单位%
#通过wset提取板块成分数据集
todayDate=datetime.datetime.strftime(datetime.date.today(),"%Y-%m-%d")
wedbdata=w.edb("M0043804", "1982-02-01",todayDate,"Fill=Previous")
print(wedbdata)
for i in range(len(wedbdata.Data[0])):
    sqllist=[]
    sqltuple=()
    sqllist.append(wedbdata.Times[i].strftime('%Y%m%d'))
    sqllist.append(wedbdata.Data[0][i])
    sqltuple=tuple(sqllist)
    cur.execute("insert into interestrate VALUES('%s','%f')"%sqltuple)
    conn.commit()

conn.close()
