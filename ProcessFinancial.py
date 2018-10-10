import pymysql
import pandas as pd
import numpy as np
import re
import sqlalchemy

def table_exists(con,table_name):#用来判断是否已经含有该表了
    sql = "show tables;"
    con.execute(sql)
    tables = [con.fetchall()]
    table_list = re.findall('(\'.*?\')',str(tables))
    table_list = [re.sub("'", '', each) for each in table_list]
    if table_name in table_list:
        return 1
    else:
        return 0
con = pymysql.connect("localhost","root","","bbt")#创建MySQL数据库连接
cursor = con.cursor()
connect_info = 'mysql://root:@localhost:3306/bbt'
engine = sqlalchemy.create_engine(connect_info)
table_name = 'financial_processed'
if(table_exists(cursor,table_name)==0):
    sql_cmd = "CREATE TABLE financial_processed SELECT secid,reportdate,totalassets,totalliab,ev FROM financial_Ori "
    cursor.execute(sql_cmd)
    print("表不存在，可以创建并且已经创建新表")
else:
    df_Financial = pd.read_sql_table("financial_processed",engine,)
    # print(df_Financial)
    df_Financial['reportdate'] = df_Financial['reportdate'].str.replace(',', '').astype(int)
    print(df_Financial.dtypes)
    for index,row in df_Financial.iterrows():
        if row["reportdate"]<20020131:
            df_Financial.drop(index,axis=0,inplace=True)
    df_Financial.to_sql(con=connect_info)
