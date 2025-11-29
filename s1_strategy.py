#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pandas as pd
from datetime import datetime
import mongo_utils
from zoneinfo import ZoneInfo

# 给定当前时间切片的数据，计算开仓信号
def generate_open_signal(current_data, top_n = 40):
    """
    先用btc的均线数据做周期判断
    在用TopN数据寻找目标标的
    """
    # 过滤掉roc_64为空或无效的数据
    valid_data = current_data.dropna(subset=['roc_64'])
    if valid_data.empty:
        return None
    
    # 找到btc的数据
    btc_data = valid_data[valid_data['symbol'] == 'BTCUSDT']
    if btc_data.empty:
        print("BTCUSDT数据不存在")
        return None
    
    # 使用btc数据计算周期：如果ma10 > ma30 则为夏季，否则为冬季
    btc_row = btc_data.iloc[0]
    ma5 = float(btc_row['ma5'])
    pre_ma5 = float(btc_row['prev_ma5'])
    ma20 = float(btc_row['ma20'])
    pre_ma20 = float(btc_row['prev_ma20'])
    ma60 = float(btc_row['ma60'])
    ma96 = float(btc_row['ma96'])
    btc_open = float(btc_row['open'])
    btc_close = float(btc_row['close'])
    btc_pre_close = float(btc_row['prev_close'])
    btc_pre_open = float(btc_row['prev_open'])
    btc_pre2_open = float(btc_row['prev2_open'])
    btc_pre2_close = float(btc_row['prev2_close'])
    
    season = None
    if ma5 < ma20 and ma5 < ma96:
    # if pre_ma5 > pre_ma20 and ma5 < ma20 and ma5 < ma60:
        season = 'winter'


    # 降序排序，取前N个，量能异常的币对
    top_df = valid_data.nlargest(top_n, 'roc_96')

    filtered_df = pd.DataFrame()
    side = None

    # 顺势做空
    if season == 'winter':
        filtered_df = top_df[
            (top_df['roc_1'] < -1)
            & (top_df['roc_1'].shift(1) < -0.5)
            & (top_df['roc_1'].shift(2) < 0)
        ].copy()
        side = 'SELL'

    if season is None or filtered_df.empty:
        print("无法判断市场周期或无可用交易信号")
        return None

    print(filtered_df)

    mongo_utils.insert_data('runtime_signal_15min_kline', filtered_df)
    
    # 选择得分最高的
    best = (filtered_df.sort_values(by='score', ascending=False)).iloc[0]
    symbol = best['symbol']
    
    print(f"选择信号: {symbol} | season={season} | side={side} | roc_64={best['roc_64']:.2f}")
    price = float(best['close']) if not pd.isna(best.get('close')) else None

    signal = {
        'symbol': symbol,
        'timestamp': int(best['timestamp']),
        'date_str': datetime.fromtimestamp(int(best['timestamp'])/1000, tz=ZoneInfo('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M:%S'),
        'price': price,
        'side': side,
        'atr': float(best['atr']) if not pd.isna(best.get('atr')) else None,
        'atr_change_pct': float(best['atr_change_pct']) if not pd.isna(best.get('atr_change_pct')) else None,
        'score': float(best['score']),
        'volume_ratio_5': float(best['volume_ratio_5']) if not pd.isna(best.get('volume_ratio_5')) else None,
        'volume_ratio_10': float(best['volume_ratio_10']) if not pd.isna(best.get('volume_ratio_10')) else None,
        'close': float(best['close']) if not pd.isna(best.get('close')) else None,
        'open': float(best['open']) if not pd.isna(best.get('open')) else None,
        'priceChangePercent': float(best.get('roc_64', 0.0)),
        'market_season': season,
        'roc_64': best.get('roc_64'),
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

    if current_symbol_data['close'] > current_symbol_data['prev_close']:
        return {
            'symbol': current_position['symbol'],
            'timestamp': int(current_symbol_data['timestamp']),
            'date_str': datetime.fromtimestamp(int(current_symbol_data['timestamp'])/1000, tz=ZoneInfo('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M:%S'),
            'side': 'SELL' if current_position['side'] == 'BUY' else 'BUY',
        }
    
