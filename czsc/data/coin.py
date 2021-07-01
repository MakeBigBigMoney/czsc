# coding: utf-8
import os
import pickle
import json
import requests
import warnings
from collections import OrderedDict
import pandas as pd
from tqdm import tqdm
from datetime import datetime, timedelta
from typing import List
from ..objects import RawBar
from ..utils.kline_generator import bar_end_time
import sqlite3
from binance_f import RequestClient
from binance_f.model import *
from binance_f.constant.test import *
import time

# 1m, 5m, 15m, 30m, 60m, 120m, 1d, 1w, 1M
freq_convert = {"1min": "1m", "5min": '5m', '15min': '15m',
                "30min": "30m", "60min": '1h', "D": "1d", "W": '1w', "M": "1M"}

conn = sqlite3.connect("trade.db")
c = conn.cursor()
DEFAULT_START_TIME = datetime(2019, 1, 1)
request_client = RequestClient(api_key=g_api_key, secret_key=g_secret_key)


# # 从数据库获取k线
# def get_kline_from_sql(symbol, interval: 'CandlestickInterval',starttime:datetime):
#     result = c.execute("SELECT * FROM MARKET_INFO WHERE SYMBOL=? AND INTERVAL=? AND DT>=? ORDER BY DT",
#                        (symbol, interval,starttime.timestamp()*1000))
#     bars = []
#     # 会有防止重复请求的
#     for row in result:
#         # if datetime.fromtimestamp(row[2]/1000)>=starttime:
#         bars.append(RawBar(symbol=symbol, dt=datetime.fromtimestamp(row[2]/1000),
#                            open=round(float(row[3]), 2),
#                            close=round(float(row[4]), 2),
#                            high=round(float(row[6]), 2),
#                            low=round(float(row[5]), 2),
#                            vol=int(row[7])))
#     return bars

def get_kline(symbol: str, end_date: [datetime, str], freq: str,
              start_date: [datetime, str] = None, count=None, fq: bool = False) -> List[RawBar]:
    """获取K线数据
    :param symbol: 币安期货的交易对 BTCUSDT/ETHUSDT
    :param start_date: 开始日期
    :param end_date: 截止日期
    :param freq: K线级别，可选值 ['1min', '5min', '30min', '60min', 'D', 'W', 'M']
    :param count: K线数量，最大值为 5000
    :param fq: 是否进行复权
    :return: pd.DataFrame
    >>> start_date = datetime.strptime("20200101", "%Y%m%d")
    >>> end_date = datetime.strptime("20210701", "%Y%m%d")
    >>> df1 = get_kline(symbol="BTCUSDT", start_date=start_date, end_date=end_date, freq="1min")
    >>> df2 = get_kline(symbol="000001.XSHG", end_date=end_date, freq="1min", count=1000)
    >>> df3 = get_kline(symbol="000001.XSHG", start_date='20200701', end_date='20200719', freq="1min", fq=True)
    >>> df4 = get_kline(symbol="000001.XSHG", end_date='20200719', freq="1min", count=1000)

    """

    # 从币安获取k线数据
    if count and count > 1300:
        warnings.warn(f"count={count}, 超过5000的最大值限制，仅返回最后5000条记录")
    end_date = datetime.now()
    result = []
    if start_date:
        start_date = pd.to_datetime(start_date)
        while len(result) == 0:
            try:
                result = request_client.get_candlestick_data(symbol=symbol,
                                                             interval=freq_convert[freq],
                                                             startTime=start_date.timestamp() * 1000,
                                                             endTime=end_date.timestamp() * 1000)
            except:
                print("重连了")
                time.sleep(2)
    elif count:
        while len(result) == 0:
            try:
                result = request_client.get_candlestick_data(symbol=symbol,
                                                         interval=freq_convert[freq],
                                                         endTime=end_date.timestamp() * 1000,
                                                         limit=count)
            except:
                print("重连了")
                time.sleep(2)
    else:
        raise ValueError("start_date 和 count 不能同时为空")

    bars = []
    for kline in result:
        bars.append(RawBar(symbol=symbol, dt=datetime.fromtimestamp(kline.openTime / 1000),
                           open=round(float(kline.open), 2),
                           close=round(float(kline.close), 2),
                           high=round(float(kline.high), 2),
                           low=round(float(kline.low), 2),
                           vol=int(float(kline.volume))))
    return bars


def get_kline_period(symbol: str, start_date: [datetime, str],
                     end_date: [datetime, str], freq: str, fq=False) -> List[RawBar]:
    """获取指定时间段的行情数据

    :param symbol: 币安期货的交易对 BTCUSDT/ETHUSDT
    :param start_date: 开始日期
    :param end_date: 截止日期
    :param freq: K线级别，可选值 ['1min', '5min', '30min', '60min', 'D', 'W', 'M']
    :param fq: 是否进行复权
    :return:
    """
    return get_kline(symbol, end_date, freq, start_date)
