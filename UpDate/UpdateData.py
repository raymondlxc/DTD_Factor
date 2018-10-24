# -*- coding:utf-8 -*-
####################################################################################################################
import pandas as pd
from WindPy import *
from sqlalchemy import create_engine
import datetime,time



class Stock():
    def __init__(self,secid,EndDate):
        self.secid=secid
        self.EndDate=EndDate

    def MarketData(self,symbol,start_date,end_date):
        print(self.getCurrentTime(),": Downloading [",symbol,"] Marketdata From "+start_date+" to "+end_date)
        w.start()
        try:
            stock=w.wsd(symbol, "open,high,low,close,volume,amt,turn", start_date,end_date)
            index_data = pd.DataFrame()
            index_data['secid']=symbol
            index_data['tradedate']=stock.Times
            index_data['openprice'] =stock.Data[0]
            index_data['highprice'] =stock.Data[1]
            index_data['lowprice']  =stock.Data[2]
            index_data['closeprice']=stock.Data[3]
            index_data['volume']=stock.Data[4]
            index_data['amt']=stock.Data[5]
            index_data['turn']=stock.Data[6]
            try:
                index_data.to_sql('stockprice',engine,if_exists='append',index=False);
            except Exception as e:
                #如果写入数据库失败，写入日志表，便于后续分析处理
                sqlerror_log=pd.DataFrame()
                sqlerror_log['secid']=symbol
                sqlerror_log['tradedate']=stock.Times
                sqlerror_log['start_date']=start_date
                sqlerror_log['end_date']=end_date
                sqlerror_log['status']='InsertError'
                sqlerror_log['table']='stockprice'
                sqlerror_log['error_info']=e
                sqlerror_log['created_date']=time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                sqlerror_log.to_sql('stock_error_log',engine,if_exists='append')
                print ( self.getCurrentTime(),": SQL Exception :%s" % (e) )
            w.start()
        except Exception as e:
            #如果读取处理失败，可能是网络中断、频繁访问被限、历史数据缺失等原因。写入相关信息到日志表，便于后续补充处理
            getdataerror_log=pd.DataFrame()
            getdataerror_log['secid']=symbol
            getdataerror_log['tradedate']=stock.Times
            getdataerror_log['start_date']=start_date
            getdataerror_log['end_date']=end_date
            getdataerror_log['status']='GetdataError'
            getdataerror_log['table']='stockprice'
            getdataerror_log['error_info']=e
            getdataerror_log['created_date']=time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
            getdataerror_log.to_sql('stock_error_log',engine,if_exists='append')
            print ( self.getCurrentTime(),":index_data %s : Exception :%s" % (symbol,e) )
            w.start()
        print(self.getCurrentTime(),": Download A Stock Marketdata Has Finished .")

    def FinancialData(self,symbol,start_date,end_date):
        print(self.getCurrentTime(),": Downloading [",symbol,"] Financialdata From "+start_date+" to "+end_date)
        w.start()
        try:
            stock=w.wsd(symbol, "tot_assets,tot_liab,net_profit_is,ev,total_shares,tot_oper_rev,eps_basic", start_date,end_date)
            index_data = pd.DataFrame()
            index_data['secid']=symbol
            index_data['reportdate']=stock.Times
            index_data['totalassets'] =stock.Data[0]
            index_data['totalliab'] =stock.Data[1]
            index_data['netprofit']  =stock.Data[2]
            index_data['ev']=stock.Data[3]
            index_data['totalshares']=stock.Data[4]
            index_data['openrev']=stock.Data[5]
            index_data['eps']=stock.Data[6]
            try:
                index_data.to_sql('financial',engine,if_exists='append',index=False);
            except Exception as e:
                #如果写入数据库失败，写入日志表，便于后续分析处理
                sqlerror_log=pd.DataFrame()
                sqlerror_log['secid']=symbol
                sqlerror_log['tradedate']=stock.Times
                sqlerror_log['start_date']=start_date
                sqlerror_log['end_date']=end_date
                sqlerror_log['status']='InsertError'
                sqlerror_log['table']='financial'
                sqlerror_log['error_info']=e
                sqlerror_log['created_date']=time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                sqlerror_log.to_sql('stock_error_log',engine,if_exists='append')
                print ( self.getCurrentTime(),": SQL Exception :%s" % (e) )
            w.start()
        except Exception as e:
            #如果读取处理失败，可能是网络中断、频繁访问被限、历史数据缺失等原因。写入相关信息到日志表，便于后续补充处理
            getdataerror_log=pd.DataFrame()
            getdataerror_log['secid']=symbol
            getdataerror_log['tradedate']=stock.Times
            getdataerror_log['start_date']=start_date
            getdataerror_log['end_date']=end_date
            getdataerror_log['status']='GetdataError'
            getdataerror_log['table']='financial'
            getdataerror_log['error_info']=e
            getdataerror_log['created_date']=time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
            getdataerror_log.to_sql('stock_error_log',engine,if_exists='append')
            print ( self.getCurrentTime(),":index_data %s : Exception :%s" % (symbol,e) )
            w.start()
        print(self.getCurrentTime(),": Download A Stock Financialdata Has Finished .")


def InterestData(start_date,end_date):
    print(self.getCurrentTime(),": Downloading Interestrate From "+start_date+" to "+end_date)
    w.start()
    try:
        stock=w.edb("M0043804", start_date,end_date,"Fill=Previous")
        index_data = pd.DataFrame()
        index_data['reportdate']=stock.Times
        index_data['R'] =stock.Data[0]
        try:
            index_data.to_sql('financial',engine,if_exists='append',index=False);
        except Exception as e:
            #如果写入数据库失败，写入日志表，便于后续分析处理
            sqlerror_log=pd.DataFrame()
            sqlerror_log['secid']='Interest'
            sqlerror_log['tradedate']=stock.Times
            sqlerror_log['start_date']=start_date
            sqlerror_log['end_date']=end_date
            sqlerror_log['status']='InsertError'
            sqlerror_log['table']='InterestRate'
            sqlerror_log['error_info']=e
            sqlerror_log['created_date']=time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
            sqlerror_log.to_sql('stock_error_log',engine,if_exists='append')
            print ( self.getCurrentTime(),": SQL Exception :%s" % (e) )
        w.start()
    except Exception as e:
        #如果读取处理失败，可能是网络中断、频繁访问被限、历史数据缺失等原因。写入相关信息到日志表，便于后续补充处理
        getdataerror_log=pd.DataFrame()
        getdataerror_log['secid']='Interest'
        getdataerror_log['tradedate']=stock.Times
        getdataerror_log['start_date']=start_date
        getdataerror_log['end_date']=end_date
        getdataerror_log['status']='GetdataError'
        getdataerror_log['table']='InterestRate'
        getdataerror_log['error_info']=e
        getdataerror_log['created_date']=time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        getdataerror_log.to_sql('stock_error_log',engine,if_exists='append')
        print (getCurrentTime(),":Exception :%s" % (e) )
        w.start()
    print(getCurrentTime(),": Download InterestRate Has Finished .")

def Stocklist(end_date):
    print(getCurrentTime(),": Downloading Stocklist Until "+end_date)
    w.start()
    wsetdata=w.wset('SectorConstituent','date='+end_date+';sectorId=0304050000000000;field=wind_code')
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
        engine.execute("insert into stocklist VALUES('%s','%s','%s','%s')"%sqltuple)

