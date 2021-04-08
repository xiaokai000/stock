import baostock as bs
import pandas as pd
from pymongo import MongoClient
from datetime import datetime, timedelta, date


collection = MongoClient(host='127.0.0.1')['stock']['all_stock']

dayStat = MongoClient(host='127.0.0.1')['stock']['dayStat']


#### 登陆系统 ####
lg = bs.login()
# 显示登陆返回信息

today = str(date.today())
yestoday = str(date.today() - timedelta(days=1))


for item in collection.find():
    stock_name = item['stock_name']

    rs = bs.query_history_k_data_plus(item['_id'],
        "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,isST",
        start_date=yestoday, end_date=today,
        frequency="d", adjustflag="3")

    #### 打印结果集 ####
    data_list = []
    while (rs.error_code == '0') & rs.next():
        # 获取一条记录，将记录合并在一起
        data_list.append(rs.get_row_data())
    result = pd.DataFrame(data_list, columns=rs.fields)
    for i in result.to_dict('records'):
        i['stock_name'] = stock_name
        _id = i['code'] + '@' + i['date']
        i['_id'] = _id
        dayStat.update({'_id': _id}, i, upsert=True)

#### 登出系统 ####
bs.logout()