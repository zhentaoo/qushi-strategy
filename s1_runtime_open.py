#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
实盘策略脚本 (1h周期)
功能：
1. 检查持仓：若有持仓，直接跳过 (不做任何操作)。
2. 若无持仓：
   - 计算开仓信号
   - 若有信号：
     - 市价买入
     - 立即下移动止损单 (TRAILING_STOP_MARKET)，回调比例 0.7 * ATR
"""

import time
import pandas as pd
from datetime import datetime
import api_core
import mongo_utils
from s1_strategy import generate_open_signal

# 配置
COLLECTION_NAME = 'runtime_symbol_factor_1h_kline'
LEVERAGE = 1         # 杠杆倍数
ORDER_MAX_AMOUNT_USDT = 3000  # 单笔下单最高金额

def get_latest_data_for_all_symbols():
    """从数据库获取所有币对最新的1条数据"""
    df = mongo_utils.query_recent_data_by_symbol(COLLECTION_NAME, limit_per_symbol=1)
    if df is None or df.empty:
        print("未获取到行情数据")
        return None
    return df

def main():
    print(f"=== {datetime.now()} 开始执行策略逻辑 ===")

    # 1. 检查持仓
    positions = api_core.get_account_position()
    has_position = False
    
    if positions:
        # 过滤掉数量为0的
        real_positions = [p for p in positions if float(p.get('positionAmt', 0)) != 0]
        if real_positions:
            has_position = True
            for p in real_positions:
                print(f"当前持仓: {p['symbol']}, 数量: {p['positionAmt']}")
    
    if has_position:
        print("当前已有持仓，策略跳过 (不进行新开仓或止损调整)")
        return

    # 2. 无持仓 -> 寻找开仓信号
    print("当前无持仓，开始寻找开仓信号...")
    
    # 获取数据
    df = get_latest_data_for_all_symbols()
    print(df)

    if df is None:
        return

    # 计算信号
    signal = generate_open_signal(df)
    # signal = {
    #     'symbol': 'HUSDT'
    # }
    
    if signal:
        symbol = signal['symbol']
        
        # 获取该币的 ATR
        symbol_row = df[df['symbol'] == symbol]
        print(symbol_row)

        atr = float(symbol_row.get('atr', 0))
        price = float(symbol_row.get('close', 0))
        
        print(f"发现开仓信号: {symbol}, 参考价: {price}, ATR: {atr}")
        
        # 计算下单金额
        balance_info = api_core.get_balance()

        available_balance = balance_info.get("availableBalance", 0) if balance_info else 0
        available = float(available_balance) * 0.9

        usdt_amount = min(available, ORDER_MAX_AMOUNT_USDT)
        
        print(f"准备下单: {symbol}, 金额: {usdt_amount} USDT")
        
        # 1. 市价开仓
        order_res = api_core.place_market_order(signal, 'BUY', usdt_amount=usdt_amount, leverage=LEVERAGE)

        if order_res and order_res.get('success'):
            print("开仓下单成功")
            positions = api_core.get_account_position()
            if positions:
                entry_price = float(positions[0].get('entryPrice', 0))
            else:
                entry_price = price

            stop_price = entry_price - (0.7 * atr)

            api_core.send_custom_wechat_message(
                f"开仓信号\n"
                f"币种: {symbol}\n"
                f"市价单·开仓价: {entry_price:.4f}\n"
                f"ATR: {atr:.4f}\n"
                f"计划初始止损线: {stop_price:.4f}\n"
                f"执行清仓"
            )
            # 通过仓位信息，获取开仓价，计算止损价
        else:
            print(f"开仓失败: {order_res.get('error') if order_res else '未知错误'}")
    else:
        print("没有符合条件的开仓信号")

if __name__ == "__main__":
    main()
