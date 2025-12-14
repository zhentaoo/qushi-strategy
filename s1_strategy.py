#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pandas as pd
from datetime import datetime
import mongo_utils
from zoneinfo import ZoneInfo

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

    print('top_df')
    print(top_df)

    # 顺势做多
    if season == 'summer':
        filtered_df = top_df[
            # (top_df['adx'] > 20)
            # & (top_df['volume_ratio_10'] > 1.1)
            # & (top_df['ma3'] > top_df['ma5'])
            # & (top_df['ma5'] > top_df['ma20'])
            # & (top_df['close'] > top_df['donchian_upper'])
            # & (top_df['close_pre1'] > top_df['donchian_upper_pre1'])
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
    """
    # 检查当前持仓是否为空
    if current_position is None:
        return None
    
    if current_symbol_data is None:
        return None

    if (
        current_symbol_data['close'] < current_symbol_data['ma5']
        or current_symbol_data['roc_1'] < current_symbol_data['ma5']
    ):
        return {
            'symbol': current_position['symbol'],
            'timestamp': int(current_symbol_data['timestamp']),
            'date_str': datetime.fromtimestamp(int(current_symbol_data['timestamp'])/1000, tz=ZoneInfo('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M:%S'),
            'side': 'SELL',
        }
    
