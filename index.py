from time import sleep
from smartapi import SmartConnect  # or from
import pandas as pd
import numpy as np;
import time

import pymongo
from datetime import  date as dateObj
import datetime


print('start time {}'.format(datetime.datetime.now()))
# CONFIGRAION
CANDLE_PERCENT = 0.05
STOCK_PRICE = 1000
CPR_NARROW_PERCENT = 0.0015

# INIT
myclient = pymongo.MongoClient(
    "mongodb+srv://admin:admin@cluster0.6ur6q.mongodb.net/msquare?retryWrites=true&w=majority")
mydb = myclient["msquare"]
trades = mydb["trades"]

obj = SmartConnect(api_key="TWOFZgdZ")
data = obj.generateSession("A201547", "Ashok7665@")
refreshToken = data['data']['refreshToken']


def cleanData(_df):
    today = dateObj.today()
    # today = dateObj.today()
    _df.drop('exchange', axis='columns', inplace=True)
    _df.drop('close', axis='columns', inplace=True)
    _df['date'] = "{}".format(today)
    _df = _df.rename(columns={'ltp': 'close', 'tradingsymbol': 'trading_symbol', 'symboltoken': 'symbol_token'})
    return _df


def calculateCPR(_df):
    _df['Pivot'] = _df.apply(lambda row: (row['high'] + row['low'] + row['close']) / 3, axis=1)
    _df['BC'] = _df.apply(lambda row: (row['high'] + row['low']) / 2, axis=1)
    _df['TC'] = _df.apply(lambda row: (row['Pivot'] - row['BC']) + row['Pivot'], axis=1)
    _df['R1'] = _df.apply(lambda row: (2 * row['Pivot']) - row['low'], axis=1)
    _df['S1'] = _df.apply(lambda row: (2 * row['Pivot']) - row['high'], axis=1)
    _df['crp_selected'] = _df.apply(lambda row: (abs((row['TC'] - row['BC'])) < CPR_NARROW_PERCENT * row['TC']), axis=1)


def fetchLastDayData(symbolToken, symbolName):
    time.sleep(0.15)
    historicData = obj.ltpData("NSE", symbolName, symbolToken)
    if historicData['data'] is None:
        return None
    history_df = pd.DataFrame([historicData['data']])
    return history_df





def lambda_handler(event, context):
    # more code here
    main_stock_list = pd.read_csv("./list_fu.csv")

    stock_last_day_data = pd.DataFrame()

    for date, row in main_stock_list.T.iteritems():
        x = fetchLastDayData(row['symboltoken'], row["tradingsymbol"])
        if x is None:
            continue
        stock_last_day_data = stock_last_day_data.append(x)

    stock_last_day_data = cleanData(stock_last_day_data)
    calculateCPR(stock_last_day_data)
    cpr_selected_trades = stock_last_day_data.loc[stock_last_day_data['crp_selected'] == True]
    dict = cpr_selected_trades.to_dict('records')

    trades_row = []

    
    for d in dict:
        trades_row.append({
            'trading_symbol': d['trading_symbol'],
            'symbol_token': d['symbol_token'],
            'date': d['date'],
            'status': 'cpr_selected'
        })

    print('SELECTED STOCKS ')
    print(trades_row)
    trades.insert_many(trades_row)



lambda_handler('test','test')