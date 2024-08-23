import numpy as np
import os
from pandas import DataFrame
import pandas as pd
from sqlalchemy import create_engine
import pymysql
from datetime import datetime, timedelta

engine = create_engine("mysql+pymysql://root:root@localhost:3306/wind_data?charset=utf8")
today = datetime.now()
yesterday = today - timedelta(days=1)
TradeDate = yesterday.strftime('%Y%m%d')
TradeDateStr = yesterday.strftime('%Y-%m-%d')
today = datetime.now()


db = pymysql.connect(host='localhost',
                     port=3306,
                     user='root',
                     password='root',
                     database='wind_data',
                     charset='utf8')
# get cursor
cursor=db.cursor()

from WindPy import w

# pre deal module
# 默认命令超时时间为120秒，如需设置超时时间可以加入waitTime参数，例如waitTime=60,即设置命令超时时间为60秒
w.start()



def get_valuation_ratio():
    sql = f'''
    select  
        t1.SEC_PETTM_MEDIA_CHN,
        t1.SEC_PB_MEDIA_CHN
    from wind_data.sec_valuation t1
    left join (
        select *
        from wind_data.section_list
        where category='板块')t2
    on t1.Codes = t2.sec_id
    where t1.business_date={TradeDate}
        and t2.sec_name='全部A股'
    '''
    res = cursor.execute(sql)
    d1 = cursor.fetchall()
    # list1 = [item for inner_tuple in d1 for item in inner_tuple]
    df1 = pd.DataFrame(d1, columns=['pe', 'pb'])

    sql = f'''
    select  
        t1.SEC_PETTM_MEDIA_CHN,
        t1.SEC_PB_MEDIA_CHN
    from wind_data.sec_valuation t1
    left join (
        select *
        from wind_data.section_list
        where category='板块')t2
    on t1.Codes = t2.sec_id
    where t1.business_date={TradeDate}
        and t2.sec_name='股转系统挂牌股票'
    '''
    res = cursor.execute(sql)
    d2 = cursor.fetchall()
    # list1 = [item for inner_tuple in d1 for item in inner_tuple]
    df2 = pd.DataFrame(d2, columns=['pe', 'pb'])

    # caculate the ratio of nq vs A-share
    df = df2 / df1
    df.rename(columns={'pe': 'pe_ratio', 'pb': 'pb_ratio'}, inplace=True)

    return df
# 获取行业估值法
def get_industry_valu_method(industry):
    sql = f'''
        select * 
        from wind_data.valuation_method
    '''
    res = cursor.execute(sql)
    columns=cursor.description
    # 显示列名
    list2=[]
    for column in columns:
        list2.append(column[0])
    d2 = cursor.fetchall()
    # list1 = [item for inner_tuple in d1 for item in inner_tuple]
    valuation_method = pd.DataFrame(d2,columns=list2)

    return valuation_method.loc[valuation_method['industry_name']==industry]['method'].iloc[0]

# 获取二级行业公司数
def get_industry2_compnum(industry):
    if industry is None:
        return 0
    sql = f'''
        select 
            t1.*, 
            substring(t2.sec_name, 5) as sec_name
        from wind_data.sec_valuation t1
        left join wind_data.section_list t2
        on t1.Codes = t2.sec_id
        where t1.business_date={TradeDate}
        and t1.category = '证监会二级行业'
    '''
    res = cursor.execute(sql)
    columns=cursor.description
    # 显示列名
    list2=[]
    for column in columns:
        list2.append(column[0])
    d2 = cursor.fetchall()
    # list1 = [item for inner_tuple in d1 for item in inner_tuple]
    df = pd.DataFrame(d2,columns=list2)
    num = df.loc[df['sec_name'] == industry]['COMPNUM'].iloc[0]

    return num

# 获取行业估值
def get_industry_valu(industry, tier=1):
    if industry is None:
        return pd.DataFrame({'pe':0,'pb':0},index=[0])
    if tier == 1:
        category = '证监会一级行业'
    else:
        category = '证监会二级行业'
    sql = f'''
        select 
            t1.SEC_PETTM_MEDIA_CHN as pe,
            t1.SEC_PB_MEDIA_CHN as pb, 
            substring(t2.sec_name, 5) as sec_name
        from wind_data.sec_valuation t1
        left join wind_data.section_list t2
        on t1.Codes = t2.sec_id
        where t1.business_date = {TradeDate}
            and t1.category = '{category}'
    '''
    res = cursor.execute(sql)
    columns=cursor.description
    # 显示列名
    list2=[]
    for column in columns:
        list2.append(column[0])
    d2 = cursor.fetchall()
    # list1 = [item for inner_tuple in d1 for item in inner_tuple]
    df = pd.DataFrame(d2,columns=list2)
    valu = df.loc[df['sec_name'] == industry]

    return valu

def windclass_2df(obj, type=0):
    df_tmp = DataFrame(obj.Data)  #pd.DataFrame()
    df_tmp = df_tmp.transpose()
    df_tmp.columns = obj.Fields

    if type == 1:
        df_tmp2 = DataFrame(obj.Codes)
        df_tmp2.columns = ['Codes']
        df_tmp = pd.concat([df_tmp2, df_tmp], axis=1)

    return df_tmp


def clean_result(dt):
    sql = f'''
        delete from  wind_data.neeq_v_show 
        where  business_date={dt};
    '''
    res = cursor.execute(sql)

    return res

def is_traday(w, natural_date):
    res = w.tdaysoffset(0, natural_date, "")
    last_traday = res.Data[0][0].strftime('%Y%m%d')
    last_traday_str = res.Data[0][0].strftime('%Y-%m-%d')

    if last_traday == natural_date or last_traday_str == natural_date:
        return 1
    else:
        return 0



print("test")