import baostock as bs
import pandas as pd
from pymongo import MongoClient

collection = MongoClient(host='127.0.0.1')['stock']['all_stock']

def download_data():
    bs.login()

    # 获取指定日期的指数、股票数据
    stock_rs = bs.query_all_stock('2020-11-11')
    stock_df = stock_rs.get_data()

    for i in stock_df.iterrows():
        stock_code = i[1][0]
        stock_name = i[1][2]
        print(i[0], stock_code, stock_name)

        if i[0] < 222 or i[0] > 4297:
            continue

        collection.update({'_id': stock_code}, {'$set': {'stock_name': stock_name}}, upsert=True)


if __name__ == '__main__':
    # 获取指定日期全部股票的日K线数据
    download_data()