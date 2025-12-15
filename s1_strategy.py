#!/usr/bin/env python3
# -*- coding: utf-8 -*-
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
    # 过滤掉roc_96为空或无效的数据
    valid_data = current_data.dropna(subset=['roc_96'])
    if valid_data.empty:
        return None

    # 降序排序，取前N个
    top_df = valid_data.nlargest(top_n, 'roc_96')
    pos_cnt = (top_df['roc_96'] > 0).sum()
    ratio = pos_cnt / top_n

    filtered_df = pd.DataFrame()

    if ratio > 0.95:
        season = 'summer'
    else:
        season = 'winter'

    side = None

    # 顺势做多
    if season == 'summer':
        filtered_df = top_df[
            (top_df['close'] > top_df['ma15'])
            & (top_df['close_pre1'] > top_df['ma15_pre1'])
            & (top_df['close_pre2'] > top_df['ma15_pre2'])
            & (top_df['close_pre3'] < top_df['ma15_pre3'])
            & (top_df['ma5'] > top_df['ma15'])
            & (top_df['volume'] > top_df['volume_ma_10'] * 3.5)
        ].copy()
        side = 'BUY'

    if season is None or filtered_df.empty:
        print("无法判断市场周期或无可用交易信号")
        return None

    # 选择得分最高的
    best = (filtered_df.sort_values(by='roc_96', ascending=False)).iloc[0]
    symbol = best['symbol']
    
    print(f"选择信号: {symbol} | season={season} | side={side}")
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
        'priceChangePercent': float(best.get('roc_96', 0.0)),
        'market_season': season,
        'roc_96': best.get('roc_96'),
    }
    return signal

# 根据当前持仓，计算平仓信号
def generate_close_signal(current_position, current_symbol_data):
    """
    生成平仓信号
    1. 固定止损：亏损5% (Check Low)
    2. ATR动态止盈：(Check Close)
    """
    # 检查当前持仓是否为空
    if current_position is None or current_symbol_data is None:
        return None

    close_price = float(current_symbol_data['close'])
    low_price = float(current_symbol_data['low'])
    entry_price = float(current_position['entry_price'])
    highest_price = float(current_position.get('highest_price'))
    atr = float(current_symbol_data.get('atr', 0))
    
    # 固定亏损5%退出
    fixed_stop_loss_price = entry_price * 0.95
    
    # ATR动态止盈退出
    atr_stop_price = highest_price - (1.4 * atr)

    # 1. 固定止损逻辑：亏损5%
    # 假设做多，价格下跌5%即止损
    if low_price <= fixed_stop_loss_price:
        return {
            'symbol': current_position['symbol'],
            'timestamp': int(current_symbol_data['timestamp']),
            'date_str': datetime.fromtimestamp(int(current_symbol_data['timestamp'])/1000, tz=ZoneInfo('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M:%S'),
            'side': 'SELL',
            'reason': 'Fixed_Stop_Loss_5pct',
            'stop_price': fixed_stop_loss_price,
            'price_type': 'fixed_stop_loss_price' # 标记是基于最低价触发
        }

    # 2. ATR动态止盈 
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
