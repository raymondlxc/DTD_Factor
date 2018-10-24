# -*- coding:utf-8 -*-
####################################################################################################################
import pandas as pd
from WindPy import *
from sqlalchemy import create_engine
import datetime,time
from UpDate import UpdateParm
from UpDate import UpdateData
from UpDate import solve

def getCurrentTime(self):
        # 获取当前时间
        return time.strftime('[%Y-%m-%d]', time.localtime(time.time()))

def getAStock():
    cur=engine.execute('SELECT secid FROM stocklist')
    results=cur.fetchall()
    return results

def getStartDate():
    cur=engine.execute('SELECT * EndDate FROM HandleLog ORDER BY EndDate DESC LIMIT 1 ')#获取操作表中最后一条记录的日期
    results=cur.fetchone()
    return results[0]

def main():
    '''
    主调函数，完成数据更新，参数计算，DTD计算
    '''
    global engine,sleep_time,symbols,start_date,end_date
    engine = create_engine('mysql+mysqldb://root:@localhost:3306/update?charset=utf8')
    start_date=getStartDate()
    end_date=getCurrentTime()
    UpdateData.InterestData()#更新利率
    UpdateData.Stocklist(start_date,end_date)#更新stocklist
    Stocklist=getAStock(end_date)
    for secid in Stocklist:
        secid=secid[0] #读取元组中的数据
        stock=UpdateData.Stock(secid,start_date,end_date)
        stock.MarketData()
        stock.FinancialData()
        cal=UpdateParm.Cal(secid,end_date)
        St=cal.getSt()
        sigma_St=stock.get_sigma_St()
        if sigma_St==-1: #若股票停牌一年以上，则退出该次循环
            continue
        F=cal.getF()
        if F==-1:#股票上市日期未能用季度报告数据进行填充
            continue
        Vt_ini=F+St#资产价值为账面市值加负债，作为方程的初值求解
        r=cal.getRate()/100
        try:
            DTD=solve.KMV(Vt_ini,St,sigma_St,F,r)
        except ValueError:
            status='fail'#错误类型为传入参数初值有误
            print(secid+'日期'+end_date+'DTD计算失败')
            sqltuple=(secid,end_date,St,sigma_St,F,Vt_ini,r,0,0,0,0,status)
            engine.execute("INSERT INTO Results1 VALUES('%s','%s','%f','%f','%f','%f','%f','%f','%f','%f','%f','%s')"%sqltuple)
        except Exception:
            status='Runtime'#以状态字判断迭代超时
            print('正在计算'+secid+'日期'+end_date+'DTD:迭代超时')
            print(DTD)
            Vt=DTD[0]
            sigma=DTD[1]
            dt=DTD[2]
            dtd=DTD[3]
            sqltuple=(secid,end_date,St,sigma_St,F,Vt_ini,r,Vt,sigma,dt,dtd,status)
            engine.execute("INSERT INTO Results1 VALUES('%s','%s','%f','%f','%f','%f','%f','%f','%f','%f','%f','%s')"%sqltuple)
        else:
            status='success'#以状态字判断迭代成功
            print('正在计算'+secid+'日期'+end_date+'DTD:成功')
            print(DTD)
            Vt=DTD[0]
            sigma=DTD[1]
            dt=DTD[2]
            dtd=DTD[3]
            sqltuple=(secid,end_date,St,sigma_St,F,Vt_ini,r,Vt,sigma,dt,dtd,status)
            engine.execute("INSERT INTO Results1 VALUES('%s','%s','%f','%f','%f','%f','%f','%f','%f','%f','%f','%s')"%sqltuple)

if __name__ == "__main__":
    main()

