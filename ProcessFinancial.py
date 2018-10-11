import pymysql
import pandas as pd
import numpy as np
import re
import sqlalchemy

def table_exists(con,table_name):#check if the table already exist
    sql = "show tables;"
    con.execute(sql)
    tables = [con.fetchall()]
    table_list = re.findall('(\'.*?\')',str(tables))
    table_list = [re.sub("'", '', each) for each in table_list]
    if table_name in table_list:
        return 1
    else:
        return 0
con = pymysql.connect("localhost","root","","bbt")#Initialize the connection to MySQL
cursor = con.cursor()
connect_info = 'mysql://root:@localhost:3306/bbt'
engine = sqlalchemy.create_engine(connect_info)
table_name = 'financial_processed'
# if(table_exists(cursor,table_name)==0):
sql_cmd = "CREATE TABLE financial_processed SELECT secid,reportdate,totalassets,totalliab,ev FROM financial_Ori "
cursor.execute(sql_cmd)
print("表不存在，可以创建并且已经创建新表")
# else:
df_Financial = pd.read_sql_table("financial_processed",engine,)
df_Financial['reportdate'] = df_Financial['reportdate'].str.replace(',', '').astype(int) #transform the date string to int
print(df_Financial.dtypes)
for index,row in df_Financial.iterrows():#remove any rows with a date before 2002
    if row["reportdate"]<20020100:
        df_Financial.drop(index,axis=0,inplace=True)
df_Financial.to_sql(name="financial_processed",con=engine,if_exists="replace",index=False)


df_financial_Completed=pd.read_sql("financial_processed",engine)
stockList = df_financial_Completed["secid"].tolist()
i=1
while i < len(stockList):#这部分用来获取financial里面的secid的list（去除重复元素）
    if stockList[i]==stockList[i-1]:
        stockList.pop(i)
        i-=1
    i=i+1
groups = dict(list(df_financial_Completed.groupby("secid")))
for k in range(0,len(stockList)):
    liabList=list(groups[stockList[k]]["totalliab"])
    dateList = list(groups[stockList[k]]["reportdate"])
    evList = list(groups[stockList[k]]["ev"])
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
            if i>notEmpty:#对于后面的值，如果等于-1，则让它等于前面的一个非-1的值
                liabList[i]=liabList[i-1]

    assetsList=list(groups[stockList[k]]["totalassets"])
    notEmpty=0#用来记录不是Empty的记录的那个值的下标
    for i in range(0,len(assetsList)-1):#得到第一个notEmpty的值的下标
        if assetsList[i]!=-1:
            notEmpty = i
            break
    for i in range(0,len(assetsList)):
        if assetsList[i]==-1:#只需要对为-1的值进行处理
            if i<notEmpty:#处理开头的值
                assetsList[i]=assetsList[notEmpty]
            if i>notEmpty:#对于后面的值，如果等于-1，则让它等于前面的一个非-1的值
                assetsList[i]=assetsList[i-1]
    dict = {"secid":secidList,"reportdate":dateList,"totalassets":assetsList,"totalliab":liabList,"ev":evList}
    df_Processed = pd.DataFrame(data=dict)
    df_Processed.to_sql(name="financial_Completed",con=engine,if_exists="append",index=False)


