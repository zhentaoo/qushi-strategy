#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from re import search
import pandas as pd
from datetime import datetime
import mongo_utils
from zoneinfo import ZoneInfo


# 开仓信号与平仓信号的区别：
# 1. 开仓信号是根据历史时间数据计算出来的
# 2. 平仓信号直接根据当前数据的涨跌给定的
# 也就是说，开仓动作是延迟的，平仓动作是即时的

# 给定当前时间切片的数据，计算开仓信号
def generate_open_signal(current_data, top_n = 30):
    """
    先用btc的均线数据做周期判断
    在用TopN数据寻找目标标的
    """
    # 过滤掉roc_64为空或无效的数据
    valid_data = current_data.dropna(subset=['roc_64'])
    if valid_data.empty:
        return None


    top_df = valid_data.nlargest(top_n, 'roc_64')
    filtered_df = pd.DataFrame()
    side = 'BUY'

    # 顺势做多
    filtered_df = top_df[
        (top_df['close'] > top_df['ma5'])
        & (top_df['close_pre1'] > top_df['ma5_pre1'])
        & (top_df['close_pre2'] > top_df['ma5_pre2'])
        & (top_df['close_pre3'] > top_df['ma5_pre3'])
        & (top_df['ma5'] > top_df['ma20'])
        & (top_df['volume'] > top_df['volume_ma_10'] * 2) #必要条件，否则胜率和收益率大幅下降
        & (top_df['volume'] < top_df['volume_ma_10'] * 7) #必要条件，否则胜率和收益率大幅下降
        & (top_df['adx'] > 45) # 329.67%
    ].copy()

    if filtered_df.empty:
        print("无法可用交易信号")
        return None

    # 选择得分最高的
    best = (filtered_df.sort_values(by='roc_64', ascending=False)).iloc[0]
    symbol = best['symbol']
    
    print(f"选择信号: {symbol} | side={side}")
    price = float(best['close']) if not pd.isna(best.get('close')) else None

    signal = {
        'symbol': symbol,
        'timestamp': int(best['timestamp']),
        'date_str': datetime.fromtimestamp(int(best['timestamp'])/1000, tz=ZoneInfo('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M:%S'),
        'price': price,
        'side': 'BUY',
        'volume_ratio_10': float(best['volume_ratio_10']) if not pd.isna(best.get('volume_ratio_10')) else None,
        'close': float(best['close']) if not pd.isna(best.get('close')) else None,
        'open': float(best['open']) if not pd.isna(best.get('open')) else None,
        'priceChangePercent': float(best.get('roc_64', 0.0)),
    }
    return signal

# 根据当前持仓，计算平仓信号
def generate_close_signal(current_position, current_symbol_data):
    """
    生成平仓信号
    ATR动态止盈：(Check Close)
    """
    # 检查当前持仓是否为空
    if current_position is None or current_symbol_data is None:
        return None

    # 历史最高
    history_highest_price = float(current_position.get('history_highest_price'))
    
    atr = float(current_symbol_data.get('atr', 0))    
    low_price = float(current_symbol_data['low'])
    
    print('****')
    print(f"当前时间: {datetime.fromtimestamp(int(current_symbol_data['timestamp'])/1000, tz=ZoneInfo('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M:%S')}")

    print(f"当前持仓: {current_position}")
    print(f"当前最高价: {history_highest_price}")
    print(f"当前最低价: {low_price}")
    print(f"当前ATR: {atr}")
    print(f"当前ATR止损价格: {history_highest_price - (0.7 * atr)}")

    print('****')

    # ATR动态止盈退出
    # atr_stop_price = history_highest_price - (1.5 * atr) # 亏损 归零
    # atr_stop_price = history_highest_price - (1.1 * atr) # 赢利 
    # atr_stop_price = history_highest_price - 1 * atr # 胜率39%，盈利700%
    # atr_stop_price = history_highest_price - 0.9 * atr # 胜率39.61%，盈利735.07%
    # atr_stop_price = history_highest_price - 0.8 * atr # 胜率39.35，盈利835.54%
    atr_stop_price = history_highest_price - 0.7 * atr # 胜率39.27%， 盈利976.82%（总收益率: 872.43%）
    # atr_stop_price = history_highest_price - 0.6 * atr # 胜率37.79%，盈利877.65%
    # atr_stop_price = history_highest_price - 0.5 * atr # 胜率33%，盈利726%
    # atr_stop_price = history_highest_price - 0.4 * atr # 胜率24.43%，归零
    # atr_stop_price = history_highest_price - 0.3 * atr # 胜率20.97%，617.30%
    # atr_stop_price = history_highest_price - 0.2 * atr # 胜率12.96，盈利307.81%

    # ATR动态止盈 
    if low_price < atr_stop_price:
        return {
            'symbol': current_position['symbol'],
            'timestamp': int(current_symbol_data['timestamp']),
            'date_str': datetime.fromtimestamp(int(current_symbol_data['timestamp'])/1000, tz=ZoneInfo('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M:%S'),
            'side': 'SELL',
            'reason': 'ATR_Trailing_Stop',
            'stop_price': atr_stop_price, # ATR止盈通常基于收盘价确认
            'price_type': 'atr_stop_price'
        }
    
    return None
    