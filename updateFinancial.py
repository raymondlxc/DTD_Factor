import pymysql
import MySQLdb
import pandas as pd
import numpy as np
import re
import sqlalchemy

def insertFinTab(connect):#用来将update的数据放入到原先的表里
    sql = "INSERT INTO financial SELECT secid, reportdate,totalassets,totalliab,ev FROM update.financial"#这个地方需要改一下，按照新的数据库设计来看的话
    connect.execute(sql)

def fillFinancial(engine):
    df_financial=pd.read_sql("financial",engine)
    stockList = list(set(df_financial["secid"].tolist()))
    groups = dict(list(df_financial.groupby("secid")))
    newSec = []  # 创建一个空列表，用来存放新上市的股票代码（目前财报全是-1的)
    for k in range(0,len(stockList)):
        liabList=list(groups[stockList[k]]["totalliab"])
        dateList = list(groups[stockList[k]]["reportdate"])
        evList = list(groups[stockList[k]]["ev"])
        assetsList = list(groups[stockList[k]]["totalassets"])
        secidList = list(groups[stockList[k]]["secid"])
        notEmpty=0#用来记录不是Empty的记录的那个值的下标
        for i in range(0,len(liabList)-1):#得到第一个notEmpty的值的下标
            if liabList[i]!=-1:
                notEmpty = i
                break
        for i in range(0,len(liabList)):
            if liabList[i]==-1:#只需要对为-1的值进行处理
                if i<notEmpty:#处理开头的值
                 liabList[i]=liabList[notEmpty]
                 assetsList[i] = assetsList[notEmpty]
                if i>notEmpty:#对于后面的值，如果等于-1，则让它等于前面的一个非-1的值
                    liabList[i]=liabList[i-1]
                    assetsList[i] = assetsList[i - 1]
        if assetsList[-1]==-1:
            newSec.append(stockList[k])
        dictFin = {"secid":secidList,"reportdate":dateList,"totalassets":assetsList,"totalliab":liabList,"ev":evList}
        df_Processed = pd.DataFrame(data=dictFin)
        df_Processed.to_sql(name="financial",con=engine,if_exists="append",index=False)
    print(newSec)
    print("成功更新Update表，并将其中的空缺月份的值填充完毕")
    return newSec

connect_info = 'mysql://root:@localhost:3306/bbt'#这里的信息视数据库名称需要进行修改
engine = sqlalchemy.create_engine(connect_info)

connection = engine.connect()
insertFinTab(connection)#先将Update数据库的里数据insert到新的表里面
fillFinancial(engine)#进行填充


