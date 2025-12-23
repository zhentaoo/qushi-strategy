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
    try:
        df = mongo_utils.query_recent_data_by_symbol('runtime_symbol_factor_1h_kline', limit_per_symbol=1)
        if df is not None and not df.empty:
            row = df[df['symbol'] == symbol]
            if not row.empty:
                atr = float(row.iloc[0].get('atr', 0))
                return atr
    except Exception as e:
        print(f"获取ATR失败: {e}")
    return 0

def get_highest_price_since_entry(symbol, entry_time_ms, entry_price):
    """
    获取开仓后的最高价格
    """
    now_ms = int(time.time() * 1000)
    duration_ms = now_ms - entry_time_ms
    
    # 选择合适的时间周期以覆盖整个持仓时间
    interval = '1m'
    if duration_ms > 600 * 60 * 1000: # > 600 mins
        interval = '1h'
        
    highest_price = entry_price
    
    # 获取K线数据
    # filter_full_time=False 以包含当前正在进行的K线
    klines_df = api_core.get_klines(symbol, interval=interval, startTime=entry_time_ms, limit=600, filter_full_time=False)
    print(len(klines_df))
    if klines_df is not None and not klines_df.empty:
        kline_high = klines_df['high'].max()
        highest_price = max(highest_price, kline_high)
        
    # 获取当前最新价格作为补充
    current_price = api_core.get_price(symbol)
    if current_price:
        highest_price = max(highest_price, current_price)
        
    return highest_price, current_price

def main():
    print(f"=== 开始执行移动止损检查 {datetime.now()} ===")
    
    # 1. 获取当前持仓
    positions = api_core.get_account_position()
    if not positions:
        print("当前无持仓")
        return

    for pos in positions:
        try:
            symbol = pos['symbol']
            raw_amt = pos['positionAmt']
            amt = float(raw_amt)
            
            # 仅处理多单 (amt > 0)
            if amt <= 0:
                # print(f"跳过空单或空仓: {symbol} amt={amt}")
                continue
                
            entry_price = float(pos['entryPrice'])
            entry_time = int(pos['updateTime'])
            
            # 2. 获取ATR
            atr = get_atr(symbol)
            if atr == 0:
                print(f"{symbol} 无法获取ATR，跳过")
                continue
                
            # 3. 获取最高价和当前价
            high_price, current_price = get_highest_price_since_entry(symbol, entry_time, entry_price)
            if not current_price:
                print(f"{symbol} 无法获取当前价格，跳过")
                continue
                
            # 4. 计算止损触发价
            # 逻辑: 如果 (最高价 - 0.7 * atr > 当前价格)，则立即清仓
            stop_loss_price = high_price - 0.7 * atr
            print(f"检查 {symbol}: 入场价={entry_price}, 最高价={high_price}, 当前价={current_price}, ATR={atr}, 止损线={stop_loss_price}")
            
            # 5. 判断是否触发止损
            if current_price < stop_loss_price:
                print(f"!!! 触发移动止损 !!! {symbol} 当前价 {current_price} < 止损线 {stop_loss_price}")
                
                # 6. 清仓
                # 注意: api_core.close_position 硬编码了 side="SELL"，适用于平多
                result = api_core.close_position(symbol, abs(amt))
                
                if result and (result.get('orderId') or result.get('msg') == 'Target position has been reduced to zero.'):
                    print(f"{symbol} 移动止损清仓成功")
                    # 发送通知
                    api_core.send_custom_wechat_message(
                        f"🛑 移动止损触发\n"
                        f"币种: {symbol}\n"
                        f"最高价: {high_price}\n"
                        f"当前价: {current_price}\n"
                        f"止损线: {stop_loss_price:.4f}\n"
                        f"ATR: {atr:.4f}\n"
                        f"执行清仓"
                    )
                else:
                    print(f"{symbol} 移动止损清仓失败: {result}")
            else:
                print(f"{symbol} 未触发止损")
                pass
                
        except Exception as e:
            print(f"处理 {pos.get('symbol')} 时发生错误: {e}")
            continue

    print("=== 检查结束 ===")

if __name__ == "__main__":
    main()
