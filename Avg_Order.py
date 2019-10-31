# -*- coding: utf-8 -*-
"""
Created on Wed Oct 23 16:09:39 2019

@author: zhouj
"""

import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import requests
import json
import tushare as ts
import datetime
import time

def GetDeclaredate():
    
    '''
    获取财报公布日期
    '''
    
    sql = "select ob_seccode_0007 as stock_code, ob_enddate_0291 as rquarter, ob_declaredate_0291 as declare_date from tb_company_0291 inner join tb_public_0007 ON tb_company_0291.OB_ORGID_0291 = tb_public_0007.OB_SECID_0007"
    db = create_engine("mysql+pymysql://readonly:User@123@rm-uf61nl564413846k9.mysql.rds.aliyuncs.com:3306/cninfo?charset=utf8")
    
    df = pd.read_sql(sql,db)
    df = df[['stock_code','rquarter','declare_date']]
    df['rquarter'] = [str(x.year) + 'Q' + str(int(x.month/3)) for x in df['rquarter']]

    
    return df

def geteehis():
    
    '''
    获取eehis表格数据
    '''
    
    sql = "select stock_id,rquarter,label_name from wind_finance.eehis"
    db_wdfinance = create_engine("mysql+pymysql://wind_finance_admin:User@123@rm-uf673a607zuetv2tj.mysql.rds.aliyuncs.com:3306/wind_finance?charset=utf8")
    
    df = pd.read_sql(sql,db_wdfinance)
    
    return df

def getBKStocks(bk_id="_a"):
    
        '''
        获取股票代码对应名称 
        '''
        
#        header_dict = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Trident/7.0; rv:11.0) like Gecko',"Content-Type": "application/json"}
        url_getname='https://stkcode.aigauss.com/HB_IDCenterWeb/JS.ashx?type=bk&cmd=%s&rtntype=1&tkn=3e715abf133fa24da68e663c5ab98857'%(bk_id)
#        req = request.Request(url=url_getname,headers=header_dict)
#        res = request.urlopen(req)
#        res = res.read()
        res = requests.get(url_getname).text
        r=json.loads(res)
        
        data=[]
        
        for temp in r:
            data.append([temp['Code'],temp['Name'],temp['ShowMkt']])
            
            
        df = pd.DataFrame(data,columns=['code','name','mkt'])
        df['stock_id'] = df['code']+'.'+ df['mkt']
        df = df[df['mkt']!='IPO']
        return df
 
def GetDailyChange():
    
    '''
    获取每日涨跌幅
    '''
    
    sql = "select OB_TRADEDATE_0160 as date, OB_SECCODE_0160 as code, F015N_0160 as Daily_Change from cninfo.tb_trade_0160 where F023V_0160 = 'A股'"
    db = create_engine("mysql+pymysql://readonly:User@123@rm-uf61nl564413846k9.mysql.rds.aliyuncs.com:3306/cninfo?charset=utf8")
    
    df = pd.read_sql(sql,db)
#    df['date'] = [x.strftime('%Y-%m-%d') for x in df['date']]    # Datetime to str

    return df   
 
def GetYesterday(): 
    
    '''获取昨日日期'''
    
    today = datetime.date.today() 
    oneday = datetime.timedelta(days = 1) 
    yesterday = today - oneday
    yesterday = yesterday.strftime('%Y-%m-%d')   
    
    return yesterday

def getorder(returns,dec_date):
    
    '''
    获取标签出现后100个交易日内收益率最低对应的序数
    '''
    
    aft = returns[returns['date'] > dec_date]
    start_ind = aft.index[0]
    
    R = returns.iloc[start_ind:start_ind + 100,:]
    R = R['Daily_Change'].tolist()
    
    index = R.index(min(R))
    
    return index

trade_calender = ts.trade_cal()    # 获取所有交易日
id_code = getBKStocks()  # 获取股票代码对应名称 
id_code['stock_code'] = id_code['code']

Declaredate = GetDeclaredate()
df = pd.merge(Declaredate,id_code,on = 'stock_code')[['stock_id','rquarter','declare_date']]

winddata = geteehis() # 获取eehis表格数据

dataset = pd.merge(df,winddata,on = ['stock_id','rquarter'],how = 'left')
dataset = dataset.dropna(axis = 0, how = 'any')  # 去除无效数据
dataset = dataset.drop_duplicates()  # 生成目标数据集

Return_mat = GetDailyChange()   # 获取涨跌幅矩阵
Return_mat['date'] = [x.strftime('%Y-%m-%d') for x in Return_mat['date']]  # 更改日期显示格式
Return_mat = Return_mat.dropna(axis = 0, how = 'any')  # 去除无效数据

yesD = GetYesterday() # 获取昨日日期

Results = []   # 生成结果矩阵
Datadict = dict()  # 生成数据字典
for i,j in dataset.groupby(['label_name']):
    subname = i
    subdata = j
    Datadict[subname] = subdata

Returndict = dict()  # 生成涨跌幅字典
for i,j in Return_mat.groupby(['code']):
    subname = i
    subdata = j
    Returndict[subname] = subdata

for i in Datadict:
    label = i
    data = Datadict[i]
    data.index = range(len(data))
    orders = []
    
    for j in range(len(data)):
        t1 = time.time()   # 计时
        code = data.iloc[j,0].split('.')[0]
        dec_date = data.iloc[j,2].strftime('%Y-%m-%d')
        
        if dec_date >= yesD:   # 剔除无效数据
            continue
        
        try:
            returns = Returndict[code]    # 获取个股的涨跌幅
            returns.index = range(len(returns))
        
        except:
            pass
        
        if dec_date >= returns.iloc[len(returns) - 1,0]:    # 剔除无效数据
            continue
        
        t = getorder(returns,dec_date)
        t2 = time.time()
        delt_t = t2 - t1   # 计时
        print(delt_t)
        orders.append(t)    # 记录序数
    
    AvgOrder = round(np.mean(orders))   # 对每一个标签得到的所有序数求平均
    Results.append([label,AvgOrder])    # 记录最终结果

# 输出结果  
output = open('Results.csv','w',encoding = 'gbk')
for item in range(len(Results)):
    output.write(Results[item][0])
    output.write('\t')
    output.write(str(Results[item][1]))
    output.write('\n')
output.close()
    

    

    
    
    
    
    
    
    
    
    