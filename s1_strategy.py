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

    market_season = 'winter' # summer是市场热的时候，也是山寨币好的时候

    BTC_data = valid_data[valid_data['symbol'] == 'BTCUSDT']
    print(BTC_data)
    btc_close = float(BTC_data['close'].iloc[0])
    btc_ma25 = float(BTC_data['ma25'].iloc[0])
    btc_ma96 = float(BTC_data['ma96'].iloc[0])
    btc_adx = float(BTC_data['adx'].iloc[0])
    btc_atr_ratio = float(BTC_data['atr'].iloc[0] / btc_close)

    if (btc_ma25 > btc_ma96 and
        18 <= btc_adx <= 30 and
        btc_atr_ratio <= 0.01):
        market_season = 'summer'

    # if market_season == 'winter':
    #     return None

    top_df = valid_data.nlargest(top_n, 'roc_64')
    filtered_df = pd.DataFrame()

    # 顺势做多
    filtered_df = top_df[
        (top_df['close'] > top_df['ma5'])
        & (top_df['close_pre1'] > top_df['ma5_pre1'])
        & (top_df['close_pre2'] > top_df['ma5_pre2'])
        & (top_df['close_pre3'] > top_df['ma5_pre3'])
        & (top_df['ma5'] > top_df['ma20'])
        & (top_df['volume'] > top_df['volume_ma_10'] * 2) # 
        # & (top_df['volume'] < top_df['volume_ma_10'] * 4) # 收益 510.39%
        & (top_df['volume'] < top_df['volume_ma_10'] * 5) # 收益 2274%
        # & (top_df['volume'] < top_df['volume_ma_10'] * 6) # 收益 999%
        & (top_df['adx'] > 40) # 329.67%
    ].copy()

    if filtered_df.empty:
        print("无法可用交易信号")
        return None

    # 选择得分最高的
    best = (filtered_df.sort_values(by='roc_64', ascending=False)).iloc[0]
    symbol = best['symbol']
    
    print(f"选择信号: {symbol}")
    price = float(best['close']) if not pd.isna(best.get('close')) else None

    signal = {
        'symbol': symbol,
        'timestamp': int(best['timestamp']),
        'date_str': datetime.fromtimestamp(int(best['timestamp'])/1000, tz=ZoneInfo('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M:%S'),
        'price': price,
        'volume_ratio_10': float(best['volume_ratio_10']) if not pd.isna(best.get('volume_ratio_10')) else None,
        'close': float(best['close']) if not pd.isna(best.get('close')) else None,
        'open': float(best['open']) if not pd.isna(best.get('open')) else None,
        'priceChangePercent': float(best.get('roc_64', 0.0)),
    }
    return signal

# 根据当前持仓，计算平仓信号，使用的是实时数据，所以要特别注意是否有问题，一些指标只能用shift 1
def generate_close_signal(current_position, current_symbol_data):
    """
    生成平仓信号
    ATR动态止盈：(Check Close)
    """
    # 检查当前持仓是否为空
    if current_position is None or current_symbol_data is None:
        return None
    
    # 核心数据
    
    # 回测必须用atr shift 1，模拟真实情况    
    entry_price = float(current_position.get('entry_price'))
    atr = min(float(current_symbol_data.get('atr_pre1', 0)), 0.03 * entry_price) 

    low_price = float(current_symbol_data['low'])
    open_price = float(current_symbol_data['open'])

    history_highest_price = float(current_position.get('history_highest_price'))


    # 斩杀线持续更新、移动
    # atr_stop_price = history_highest_price - 0.6 * atr # 172.24%
    atr_stop_price = history_highest_price - 0.7 * atr # 274.20%
    # atr_stop_price = history_highest_price - 1.2 * atr # 169.71%

    # 如果下一个k线开盘价低于ATR止损价，就平仓
    if open_price <= atr_stop_price:
        print(f"当前时间: {datetime.fromtimestamp(int(current_symbol_data['timestamp'])/1000, tz=ZoneInfo('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"当前最高价: {history_highest_price}")
        print(f"当前最低价: {low_price}")
        print(f"当前ATR: {atr}")
        print(f"当前ATR止损价格 open_price: {open_price}")
        return {
            'symbol': current_position['symbol'],
            'timestamp': int(current_symbol_data['timestamp']),
            'date_str': datetime.fromtimestamp(int(current_symbol_data['timestamp'])/1000, tz=ZoneInfo('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M:%S'),
            'atr': atr,
            'stop_price': open_price,
        }
    
    # 如果下一个k线开盘比atr高，但是最低价比atr止损价低，就平仓
    if low_price <= atr_stop_price:
        print(f"当前时间: {datetime.fromtimestamp(int(current_symbol_data['timestamp'])/1000, tz=ZoneInfo('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"当前持仓: {current_position}")
        print(f"当前最高价: {history_highest_price}")
        print(f"当前最低价: {low_price}")
        print(f"当前ATR: {atr}")
        print(f"当前ATR止损价格 atr_stop_price: {atr_stop_price}")
        return {
            'symbol': current_position['symbol'],
            'timestamp': int(current_symbol_data['timestamp']),
            'date_str': datetime.fromtimestamp(int(current_symbol_data['timestamp'])/1000, tz=ZoneInfo('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M:%S'),
            'atr': atr,
            'stop_price': atr_stop_price,
        }

    return None
    