#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
止损守护脚本 (1分钟周期)
"""

import time
import api_core
import mongo_utils
from datetime import datetime

def get_atr(symbol):
    """从数据库获取最新的 ATR"""
    # 获取最近的一条数据
    df = mongo_utils.query_recent_data_by_symbol('runtime_symbol_factor_1h_kline', limit_per_symbol=1)
    if df is not None and not df.empty:
        row = df[df['symbol'] == symbol]
        if not row.empty:
            atr = float(row.iloc[0].get('atr', 0))
            return atr
    return 0

def get_highest_price_since_entry(symbol, entry_time_ms):
    """
    获取自入场以来的最高价格
    
    Args:
        symbol: 交易对
        entry_time_ms: 入场时间戳(毫秒)
    """
    try:
        # 获取当前价格作为基准
        current_price = api_core.get_price(symbol)
        if current_price is None:
            return None
        
        highest_price = current_price

        # 获取自入场以来的K线数据 (1分钟K线)
        # 限制获取最近1000根，覆盖约16小时，通常足够。如果持仓更久，可能需要分段获取，
        # 但考虑到这是守护脚本，主要关注近期高点。
        klines_df = api_core.get_klines(symbol, interval="1m", startTime=entry_time_ms, limit=999)
        
        if klines_df is not None and not klines_df.empty:
            # 获取K线中的最高价最大值
            kline_high = klines_df['high'].max()
            if kline_high > highest_price:
                highest_price = kline_high
                
        return highest_price
    except Exception as e:
        print(f"获取最高价失败 {symbol}: {e}")
        return None

def main():
    print(f"=== 止损守护脚本启动 {datetime.now()} ===")
    
    # 1. 获取当前持仓
    positions = api_core.get_account_position()
    if not positions:
        print("当前无持仓")
        return

    for pos in positions:
        symbol = pos.get('symbol')
        position_amt = float(pos.get('positionAmt', 0))
        entry_price = float(pos.get('entryPrice', 0))
        # updateTime 可能是最近更新时间，近似作为入场时间
        update_time = int(pos.get('updateTime', 0)) 
        
        if position_amt <= 0:
            print(f"跳过空头或空仓位: {symbol} {position_amt}")
            continue

        print(f"正在检查持仓: {symbol}, 入场价: {entry_price}")

        # 2. 获取 ATR
        atr = get_atr(symbol)
        if atr <= 0:
            print(f"无法获取有效 ATR: {symbol}")
            continue
        print(f"当前 ATR: {atr}")

        # 3. 获取入场后最高价
        highest_price = get_highest_price_since_entry(symbol, update_time)
        if highest_price is None:
            print(f"无法获取最高价: {symbol}")
            continue
        
        print(f"入场后最高价: {highest_price}")


    print("=== 检查结束 ===")

if __name__ == "__main__":
    main()
