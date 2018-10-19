import pymysql
import pandas as pd
import numpy as np
import re
import sqlalchemy
connect_info = 'mysql://root:@localhost:3306/bbt'
engine = sqlalchemy.create_engine(connect_info)
def getStockList(connect_info,engine,table_name):#输入mysql数据库链接以及engine，以及对应的表名
    connect_info =connect_info
    engine = engine
    df_financial=pd.read_sql(table_name,engine)
    stockList = df_financial["secid"].tolist()
    i=1
    while i < len(stockList):#这部分用来获取financial里面的secid的list（去除重复元素）
        if stockList[i]==stockList[i-1]:
            stockList.pop(i)
            i-=1
        i=i+1
    return stockList #return the complete stocklist
stockList = getStockList(connect_info,engine,table_name = "financial")
print(len(stockList))