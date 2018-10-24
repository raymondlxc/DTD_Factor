import pymysql
import MySQLdb
import pandas as pd
import numpy as np
import re
import sqlalchemy

connect_info = 'mysql://root:@localhost:3306/update'
engine = sqlalchemy.create_engine(connect_info)
connection = engine.connect()
stockSet = set(pd.read_sql("stocklist",engine)["secid"].tolist())

def getStockList(engine,table_name):#输入mysql数据库链接以及engine，以及对应的表名
    df_financial=pd.read_sql(table_name,engine)
    stockList = df_financial["secid"].tolist()
    stockList = list(set(stockList))
    return stockList #return the complete stocklist

def checkUpdate(table_name,engine):#输入Update数据库里的表名字，输入连接，判断该表是否Update完整
    stocksInTable = set(getStockList(engine, table_name))
    if(stockSet==stocksInTable):print("The update is successful!")
    else: print("The update is not successful")
    return stockSet==stocksInTable




