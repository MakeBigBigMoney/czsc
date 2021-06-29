# coding: utf-8
"""
目前 czsc 库处于开发阶段，不同版本之间的 API 兼容性较差。
这个文件对应的 czsc 版本为 0.7.2，代码即文档，关于0.7.2的所有你想知道的都在代码里。

注意：czsc 是针对程序化实盘进行设计的，用来做研究需要自己按需求改动代码，强烈建议研究、实盘使用统一的代码。
"""
from binance_f import RequestClient
from binance_f.model import *
from binance_f.constant.test import *
from binance_f.base.printobject import *
import time
import czsc
from czsc.factors import CzscTrader
# 聚宽数据为目前支持的数据源，需要接入第三方数据源的请参考这个文件进行编写
from czsc.data.jq import *
import pandas as pd
import traceback
import tushare as ts
from datetime import datetime, timedelta
from typing import List
from czsc.analyze import CZSC, RawBar
from czsc.enum import Signals, Freq
from czsc.factors.utils import match_factor, match_factors
import sqlite3

assert czsc.__version__ == '0.7.2'
conn = sqlite3.connect("trade.db")
c = conn.cursor()
DEFAULT_START_TIME = datetime(2019, 1, 1)
request_client = RequestClient(api_key=g_api_key, secret_key=g_secret_key)


# 获取最后一条K线的级别 返回的是datetime
def get_last_kline_time(symbol, interval) -> datetime:
    last_k_time = DEFAULT_START_TIME
    result = c.execute("SELECT LAST_TIME FROM MARKET_LAST_UPDATE WHERE SYMBOL=? AND INTERVAL =?", (symbol, interval))
    for row in result:
        last_k_time = datetime.fromtimestamp(row[0]/1000)
    return last_k_time


# 插入数据库
def insert_into_sql(klines: List[RawBar], interval):
    if len(klines) == 0:
        return
    last_k_time = get_last_kline_time(klines[0].symbol, interval)
    for kline in klines:
        if kline.dt == last_k_time:
            c.execute("update MARKET_INFO set close=?,low=?,high=? where dt=?",
                      (
                          kline.close,
                          kline.low,
                          kline.high,
                          kline.dt.timestamp() * 1000
                      ))
        elif kline.dt > last_k_time:
            c.execute("INSERT INTO MARKET_INFO VALUES(?,?,?,?,?,?,?,?,?)", (
                None,
                kline.symbol,
                kline.dt.timestamp()*1000,
                kline.open,
                kline.close,
                kline.low,
                kline.high,
                float(kline.vol),
                interval
            ))
    # 更新插入时间
    # 如果存在就更新 不存在就插入
    result = c.execute("select * from MARKET_LAST_UPDATE WHERE SYMBOL=? AND INTERVAL =?", (klines[0].symbol, interval))
    if len(list(result)) != 0:
        c.execute("UPDATE MARKET_LAST_UPDATE SET LAST_TIME=? WHERE SYMBOL=? AND INTERVAL =?",
                  (klines[len(klines) - 1].dt.timestamp()*1000, klines[0].symbol, interval))
    else:
        c.execute("INSERT INTO MARKET_LAST_UPDATE values(?,?,?,?)",
                  (klines[0].symbol, interval, klines[len(klines) - 1].dt.timestamp()*1000, None))

    conn.commit()

    # ======================================================================================================================


def get_kline_from_sql(symbol, interval: 'CandlestickInterval',starttime:datetime):
    result = c.execute("SELECT * FROM MARKET_INFO WHERE SYMBOL=? AND INTERVAL=? AND DT>=? ",
                       (symbol, interval,starttime.timestamp()*1000))
    bars = []
    # 会有防止重复请求的
    for row in result:
        # if datetime.fromtimestamp(row[2]/1000)>=starttime:
        bars.append(RawBar(symbol=symbol, dt=datetime.fromtimestamp(row[2]/1000),
                           open=round(float(row[3]), 2),
                           close=round(float(row[4]), 2),
                           high=round(float(row[6]), 2),
                           low=round(float(row[5]), 2),
                           vol=int(row[7])))
    return bars


# 先从数据库获取 再从币安获取
def get_kline(symbol, interval: 'CandlestickInterval', startTime: datetime):
    # 这样的话 即使第一次使用 也能把所有数据更新下来 虽然是看上去冗余了些
    get_kline_remote(symbol, interval, startTime)
    bars = get_kline_from_sql(symbol, interval,startTime)
    return bars

# todo更新k线信息 使用websocket
def update_kline(symbol,interval:'CandlestickInterval'):
    pass



# 从币安获取k线数据
def get_kline_remote(symbol, interval: 'CandlestickInterval', startTime: datetime):
    bars = []
    last_k_time = get_last_kline_time(symbol, interval)
    last_end_time = last_k_time
    # 因为第一次想要把所有的数据下载下来 所以第一次就用默认的时间
    print(symbol + interval + " 最后一次更新时间:" + last_end_time.strftime("%Y-%m-%d %H:%M:%S"))
    # if last_k_time != DEFAULT_START_TIME and startTime > last_k_time:
    #     print("仅更新数据")
    #     last_end_time = startTime
    Condition1 = True
    while Condition1:
        try:
            result = request_client.get_candlestick_data(symbol="BTCUSDT", interval=interval,

                                                 startTime=last_end_time.timestamp() * 1000,limit=100)
            for kline in result:
                if kline.openTime / 1000 >= last_end_time.timestamp():
                    bars.append(RawBar(symbol=symbol, dt=datetime.fromtimestamp(kline.openTime / 1000),
                                       open=round(float(kline.open), 2),
                                       close=round(float(kline.close), 2),
                                       high=round(float(kline.high), 2),
                                       low=round(float(kline.low), 2),
                                       vol=int(float(kline.volume))))
                elif kline.openTime / 1000 < last_end_time.timestamp():
                    Condition1 = False

            if len(result) == 0:
                # if (datetime.now() - last_end_time).days>3:
                #     time.sleep(2)
                #     continue
                break
            last_end_time = datetime.fromtimestamp(result[len(result) - 1].closeTime / 1000)
        except:
            print("休息两秒，重连")
            time.sleep(2)
            continue

    insert_into_sql(bars, interval)
    return bars

def show_bi(symbol, interval: 'CandlestickInterval', starttime:datetime):
    bars = get_kline(symbol, interval, starttime)
    c = CZSC(bars, freq=interval, max_bi_count=1000)
    c.open_in_browser()
    # c.update()

def main():
    # 获取k线
    # show_bi("BTCUSDT", CandlestickInterval.DAY1, datetime(2020, 4, 1))
    # show_bi("BTCUSDT", CandlestickInterval.HOUR4, datetime(2020, 4, 1))
    # show_bi("BTCUSDT", CandlestickInterval.MIN30, datetime(2021, 4, 1))
    # show_bi("BTCUSDT", CandlestickInterval.MIN15, datetime(2021, 4, 1))
    # show_bi("BTCUSDT", CandlestickInterval.MIN5, datetime(2021, 6, 1))
    show_bi("BTCUSDT", CandlestickInterval.MIN1, datetime(2021, 6, 29))

if __name__ == '__main__':
    main()
