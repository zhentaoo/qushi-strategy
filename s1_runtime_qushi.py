#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
import time
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import api_core
import mongo_utils
from s1_strategy import generate_signal

# 交易参数配置
KLINE_INTERVAL = "15m"  # K线周期（15分钟）

# 定时脚本初始化的时候执行，先平仓+清空委托单，后续再进行选信号，下单逻辑
def close_all_positions_if_any():
    """
    若发现有持仓，先全部平仓（市价 reduceOnly）。
    若没有持仓但存在挂单，按交易对逐个撤销全部挂单。
    返回是否执行了平仓操作。
    """
    try:
        # 1. 每次脚本执行：先撤销所有挂单，然后再获取仓位进行平仓
        api_core.cancel_all_open_orders()

        # 2. 检查并平仓所有持仓
        active_positions = api_core.get_account_position()

        print(f"发现 {len(active_positions)} 个持仓，开始依次平仓")
        for position in active_positions:
            symbol = position.get('symbol')
            try:
                position_amt = float(position.get('positionAmt', 0))
            except Exception:
                position_amt = 0
            if not symbol or position_amt == 0:
                continue
            print(f"平仓 {symbol}: 数量 {abs(position_amt)}")
            try:
                close_result = api_core.close_position(symbol, position_amt)
                print(f"平仓结果: {close_result}")
            except Exception as e:
                print(f"平仓 {symbol} 失败: {e}")
        print("已完成所有持仓的平仓处理")
        return True
    except Exception as e:
        print(f"检查并平仓持仓失败: {e}")
        return False

def main():
    print(f"=== {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}  开始执行 == ")

    # 1.先平掉之前的仓位：本轮开始若发现有持仓，则全部先平仓
    close_all_positions_if_any()

    # 2. 从因子集合计算交易信号（s4核心逻辑）
    df = mongo_utils.query_recent_data_by_symbol('runtime_symbol_factor_15min_kline', limit_per_symbol=1)
    signal = generate_signal(df, 30)
    print(signal)
    if signal is None:
        print("信号计算失败或不满足条件，退出")
        return

    side = signal.get('side')

    # 3. 下单：根据账户可用余额动态设置下单金额与杠杆（分档规则）
    balance_info = api_core.get_balance()
    available = 0.0
    try:
        if balance_info and isinstance(balance_info, dict):
            available = float(balance_info.get('availableBalance', 0))
    except Exception:
        available = 0.0

    leverage = 1
    order_usdt = min(available * leverage * 0.92, 4000)
    order_result = api_core.place_order(signal, side, usdt_amount=order_usdt, leverage=leverage)
    
    # 4. 止盈止损单：同步下 40% 止损平仓单（STOP_MARKET, reduceOnly）
    stop_loss_price_ratio = 0.4
    take_profit_price_ratio = 0.4

    symbol = signal.get('symbol')
    latest_price = api_core.get_price(symbol)

    if side == "SELL":
        stop_loss_price = latest_price * (1 + stop_loss_price_ratio)
        take_profit_price = latest_price * (1 - take_profit_price_ratio)
    
    if side == "BUY":
        stop_loss_price = latest_price * (1 - stop_loss_price_ratio)
        take_profit_price = latest_price * (1 + take_profit_price_ratio)

    if order_result and order_result.get('success'):
        try:
            symbol = order_result.get('symbol')
            # 获取最新持仓以准确读取入场价与数量
            positions = api_core.get_account_position()
            entry_price = None
            position_qty = None
            if positions:
                for p in positions:
                    if p.get('symbol') == symbol:
                        try:
                            position_qty = abs(float(p.get('positionAmt', 0)))
                        except Exception:
                            position_qty = None
                        try:
                            ep = float(p.get('entryPrice', 0))
                            if ep > 0:
                                entry_price = ep
                        except Exception:
                            entry_price = None
                        break

            # 兜底逻辑移除：严格依赖持仓信息
            if entry_price and position_qty and position_qty > 0:
                sl_res = api_core.place_tp_sl_order(
                    symbol=symbol,
                    originSide= side,
                    quantity=position_qty,
                    take_profit_price=take_profit_price,
                    stop_loss_price=stop_loss_price
                )
                print(f"8%止损委托结果: {sl_res}")
            else:
                print("无法设置止损：缺少入场价或数量信息")
        except Exception as e:
            print(f"设置 8% 止损失败: {e}")

    # 5.发送微信通知
    api_core.send_wechat_message(signal, order_result)

    print("=== 执行完成 ===")

if __name__ == "__main__":
    # 改为一次性执行脚本：直接运行主流程
    print('run python script')
    main()