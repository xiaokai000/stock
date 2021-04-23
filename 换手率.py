import baostock as bs
import pandas as pd
from pymongo import MongoClient
from datetime import datetime, timedelta, date

dayStat = MongoClient(host='127.0.0.1')['stock']['dayStat']

yestoday = str(date.today() - timedelta(days=1))
##############
##### 换手率 5-10
##############

print(yestoday)
for i in dayStat.find({'turn': {'$gt': 0, '$lt': 10}}):
    print(i)
