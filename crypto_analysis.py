# -*- coding: utf-8 -*-
"""
Created on Sat Aug 15 15:07:59 2020

@author: kon
"""

import requests
import pandas as pd
import numpy as np
import datetime as dt
from functools import reduce
import os
import getpass

username = getpass.getuser()

directory = f'C:\\Users\\{username}\\Desktop'

os.chdir(directory)

def strToday():
    today = dt.date.today()
    return dt.datetime.strftime(today, "%Y-%m-%d")

# Base URL for all public and private APIs:
base_url = 'https://api.btcmarkets.net'
fslash = '/'
version = 'v3'
category = 'markets'
sub_category = 'candles'
from_date = '2013-01-01T00:00:00.000000Z' # allows retrieving market candles from a specific time. The value must be timestamp in ISO 8601 format
to_date = f'{strToday()}T00:00:00.000000Z' # allows retrieving market candles from a specific time. The value must be timestamp in ISO 8601 format
tickers_raw = ['BTC', 'XRP', 'ETH', 'LTC', 'BCH', 'BSV', 'ENJ', 'COMP', 'BAT', 'POWR', 'XLM', 'OMG', 'LINK']
tickers = [f'{ticker}-AUD' for ticker in tickers_raw]

def price_history(ticker):
    url = f'https://api.btcmarkets.net/v3/markets/{ticker}/candles?from={from_date}&to={to_date}'
    data_raw = requests.get(url).json() # opening up API in json format; output is a dictionary
    #data_clean = {'Date': [n[0] for n in data_raw], 'Close': [n[-2] for n in data_raw]}
    data_clean = {'Date': [n[0][0:len("YYYY-MM-DD")] for n in data_raw], f'Close ({ticker})': [n[-2] for n in data_raw]}
    df = pd.DataFrame(data_clean)
    #df['Date'] = pd.to_datetime(df['Date'])
    df['Date'] = df['Date'].apply(lambda i: dt.datetime.strptime(i, "%Y-%m-%d"))
    return df

dataframe_list = [price_history(ticker) for ticker in tickers]
df_final = reduce(lambda  df_left , df_right: pd.merge(df_left , df_right, on = ['Date'] , how = 'outer'), dataframe_list)
df_final = df_final.sort_values(by = 'Date').reset_index(drop = True)

dict_quarters = {'01': 'Quarter 1', '02': 'Quarter 1', '03': 'Quarter 1',
                 '04': 'Quarter 2', '05': 'Quarter 2', '06': 'Quarter 2',
                 '07': 'Quarter 3', '08': 'Quarter 3', '09': 'Quarter 3',
                 '10': 'Quarter 4', '11': 'Quarter 4', '12': 'Quarter 4'}
dict_months = {'01': 'Jan', '02': 'Feb', '03': 'Mar', '04': 'Apr',
               '05': 'May', '06': 'Jun', '07': 'Jul', '08': 'Aug',
               '09': 'Sep', '10': 'Oct', '11': 'Nov', '12': 'Dec'}

def percentage_change(ticker, quarterly=True):
    headerClosingPrice = f'Close ({ticker})'
    headerPercentageChange = f'PctChange ({ticker})'
    headerMonth = 'Month'
    headerMonthNew = 'Month New'
    headerMonthYear = 'Month Year'
    headerQuarter = 'Quarter'
    headerQuarterYear = 'Quarter Year'
    headerDate = 'Date'
    headerMean = 'Mean'
    headerVolatility = 'Standard Deviation'
    df1 = df_final.loc[ : , [headerDate, headerClosingPrice] ]
    df2 = df1.dropna().reset_index(drop = True)
    df2[headerClosingPrice] = df2[headerClosingPrice].astype(float)
    df2[headerPercentageChange] = df2[headerClosingPrice].pct_change()
    df3 = df2.dropna().reset_index(drop = True)
    df3[headerMonth] = [str(i)[len("XXXX-"):len("XXXX-")+2] for i in df3[headerDate]]
    if quarterly:
        df3[headerQuarter] = df3[headerMonth].replace(dict_quarters)
        df3[headerQuarterYear] = [f'{quarter} {str(date.year)}' for quarter, date in zip(list(df3[headerQuarter]), list(df3[headerDate])) ]
        df4 = df3.loc[ : , [headerQuarterYear, headerPercentageChange]]
        df5 = df4.groupby(headerQuarterYear, as_index = False)[headerPercentageChange].aggregate({headerMean: np.mean, headerVolatility: np.std})
        df6 = df5.sort_values(by = headerVolatility, ascending = False)
        df6.to_excel(f'{ticker} Stats - Quarterly.xlsx', index = False)
    else:
        df3[headerMonthNew] = df3[headerMonth].replace(dict_months)
        df3[headerMonthYear] = [f'{month} {str(date.year)}' for month, date in zip(list(df3[headerMonthNew]), list(df3[headerDate])) ]
        df4 = df3.loc[ : , [headerMonthYear, headerPercentageChange]]
        df5 = df4.groupby(headerMonthYear, as_index = False)[headerPercentageChange].aggregate({headerMean: np.mean, headerVolatility: np.std})
        df6 = df5.sort_values(by = headerVolatility, ascending = False)
        df6.to_excel(f'{ticker} Stats - Monthly.xlsx', index = False)
        
for ticker in tickers:
    for boolean in [True, False]:
        percentage_change(ticker = ticker, quarterly = boolean) 


