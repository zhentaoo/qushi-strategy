#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import time
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
import mongo_utils
import factor_utils
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pytz
from s1_strategy import generate_open_signal, generate_close_signal

# 设置中国时区
CHINA_TZ = pytz.timezone('Asia/Shanghai')

plt.rcParams['font.sans-serif'] = ['SimHei', 'Arial Unicode MS', 'DejaVu Sans']
plt.rcParams['axes.unicode_minus'] = False

interval = '1h'  # 改为1h K线

# 生成时间窗口
def get_time_windows(df, window_min=60):
    """
    生成时间窗口，每60分钟一个窗口
    """
    if df.empty:
        return []
    
    # 获取数据的时间范围
    min_time = df['timestamp'].min()
    max_time = df['timestamp'].max()
    print(f"数据时间范围: {min_time} 到 {max_time}")
    
    
    # 生成时间窗口
    window_ms = window_min * 60 * 1000  # 60分钟的毫秒数
    
    windows = []
    current_time = min_time
    while current_time <= max_time:
        windows.append(current_time)
        current_time += window_ms
    
    print(f"生成 {len(windows)} 个时间窗口")
    return windows

# 绘图与统计
def plot_backtest_results(trades_data, save_path='./record-md/account_curve.png', show_plot=True, initial_balance=1000):
    print("开始绘制交易收益曲线")
    
    if isinstance(trades_data, list):
        df = pd.DataFrame(trades_data)
    else:
        df = trades_data.copy()
    
    if df is None or df.empty:
        return None
    
    df['datetime'] = pd.to_datetime(df['trade_close_timestamp'], unit='ms')
    df = df.sort_values('datetime').reset_index(drop=True)
    df['cumulative_balance'] = df['final_balance']
    
    df['cumulative_profit'] = df['cumulative_balance'] - initial_balance
    df['cumulative_return_pct'] = (df['cumulative_profit'] / initial_balance) * 100
    df['peak'] = df['cumulative_balance'].cummax()
    df['drawdown'] = (df['cumulative_balance'] - df['peak']) / df['peak'] * 100
    
    fig, ax = plt.subplots(1, 1, figsize=(12, 8))
    fig.suptitle('账户余额变化', fontsize=16, fontweight='bold')
    
    ax.plot(df['datetime'], df['cumulative_balance'], 
            color='#2E86AB', linewidth=2, label='账户余额')
    ax.axhline(y=df['cumulative_balance'].iloc[0], 
               color='red', linestyle='--', alpha=0.7, label='初始资金')
    ax.set_title('账户余额变化', fontsize=14, fontweight='bold')
    ax.set_ylabel('余额 (USDT)', fontsize=12)
    ax.set_xlabel('日期', fontsize=12)
    ax.grid(True, alpha=0.3)
    ax.legend()
    
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
    ax.xaxis.set_major_locator(mdates.DayLocator(interval=7))
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)
    
    plt.tight_layout()
    plt.savefig(save_path, dpi=300, bbox_inches='tight')
    print(f"图表已保存到: {save_path}")
    
    if show_plot:
        plt.show()
    else:
        plt.close()

    initial_balance = float(initial_balance)
    final_balance = df['cumulative_balance'].iloc[-1]
    total_profit = final_balance - initial_balance
    total_return = (total_profit / initial_balance) * 100
    
    win_trades = df[df['profit'] > 0]
    lose_trades = df[df['profit'] < 0]
    win_rate = len(win_trades) / len(df) * 100
    
    max_profit = df['profit'].max()
    max_loss = df['profit'].min()
    avg_profit = df['profit'].mean()
    max_drawdown = df['drawdown'].min()
    
    print("\n" + "="*50)
    print("交易统计信息")
    print("="*50)
    print('symbol:', df['symbol'].iloc[0])
    print(f"初始资金: {initial_balance:,.2f} USDT")
    print(f"最终资金: {final_balance:,.2f} USDT")
    print(f"总收益: {total_profit:,.2f} USDT")
    print(f"总收益率: {total_return:.2f}%")
    print(f"交易次数: {len(df)}")
    print(f"胜率: {win_rate:.2f}%")
    print(f"平均盈亏: {avg_profit:.2f} USDT")
    print(f"最大单笔盈利: {max_profit:.2f} USDT")
    print(f"最大单笔亏损: {max_loss:.2f} USDT")
    print(f"最大回撤: {max_drawdown:.2f}%")
    print("="*50)

# 回测循环
def main():
    # 0. 清空历史记录表
    mongo_utils.delete_data('trade_records')
    mongo_utils.delete_data('factor_processed_kline')


    # 1. 从MongoDB获取1h K线数据
    start_time = time.time()
    
    # 24 + 25年
    # kline_df = mongo_utils.query_data_by_timestamp('symbol_1h_kline', '2024-02-01', '2025-12-01') # 183837.86%

    # 24年：
    # kline_df = mongo_utils.query_data_by_timestamp('symbol_1h_kline', '2024-02-01', '2025-01-01') # 608.65%
    
    # 25年：
    # kline_df = mongo_utils.query_data_by_timestamp('symbol_1h_kline', '2025-01-01', '2025-12-01') #  10593.37%
    # kline_df = mongo_utils.query_data_by_timestamp('symbol_1h_kline', '2025-06-01', '2025-12-01') # adx pre1，23171.92%，23171.92
    # kline_df = mongo_utils.query_data_by_timestamp('symbol_1h_kline', '2025-06-01', '2025-07-01') # adx pre1，23171.92%，23171.92
    kline_df = mongo_utils.query_data_by_timestamp('symbol_1h_kline', '2025-01-01', '2025-07-01') # adx pre1，23171.92%，23171.92
    
    # 按月份
    # kline_df = mongo_utils.query_data_by_timestamp('symbol_1h_kline', '2025-01-01', '2025-02-01') #
    # kline_df = mongo_utils.query_data_by_timestamp('symbol_1h_kline', '2025-02-01', '2025-03-01') #
    # kline_df = mongo_utils.query_data_by_timestamp('symbol_1h_kline', '2025-03-01', '2025-04-01') #
    # kline_df = mongo_utils.query_data_by_timestamp('symbol_1h_kline', '2025-04-01', '2025-05-01') #
    # kline_df = mongo_utils.query_data_by_timestamp('symbol_1h_kline', '2025-05-01', '2025-06-01') #
    # kline_df = mongo_utils.query_data_by_timestamp('symbol_1h_kline', '2025-06-01', '2025-07-01') #
    # kline_df = mongo_utils.query_data_by_timestamp('symbol_1h_kline', '2025-07-01', '2025-08-01') #
    # kline_df = mongo_utils.query_data_by_timestamp('symbol_1h_kline', '2025-08-01', '2025-09-01') #
    # kline_df = mongo_utils.query_data_by_timestamp('symbol_1h_kline', '2025-09-01', '2025-10-01') #
    # kline_df = mongo_utils.query_data_by_timestamp('symbol_1h_kline', '2025-10-01', '2025-11-01') #
    # kline_df = mongo_utils.query_data_by_timestamp('symbol_1h_kline', '2025-11-01', '2025-12-01') #
    
    
    # 计算因子数据
    print('开始计算指标')
    processed_df = factor_utils.compute_symbol_factor(kline_df)
    mongo_utils.insert_data('factor_processed_kline',processed_df)

    elapsed = time.time() - start_time
    print(f"K线数据长度: {len(kline_df)}")
    print(f"读取1h K线数据耗时: {elapsed:.2f}秒")


    # 2. 生成时间窗口
    time_windows = get_time_windows(processed_df)
    print(time_windows) 
    print(len(time_windows))

    # 3. 账户初始化回测参数
    cash_balance_start = 1000.0
    cash_balance = 1000.0
    trade_records_list = []  # Changed to list for performance
    current_position = None
    
    print(f"开始回测，初始资金: {cash_balance_start} USDT")
    print(f"时间窗口数量: {len(time_windows)}")
    
    # 4. 回测大时间循环
    for idx in range(100, len(time_windows) - 1):
        # 当前时间切面，所有币对数据
        current_window_time = time_windows[idx]
        current_timestamp_data = processed_df[processed_df['timestamp'] == current_window_time]

        # 当前时间窗口，所有币对的 指标/因子数据
        print(current_window_time)
        print(f"\n=== 时间窗口 {idx}/{len(time_windows)-1} ===")
        print(f"当前时间: {datetime.fromtimestamp(current_window_time/1000, tz=CHINA_TZ).strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"当前账户余额: {cash_balance:.2f} USDT")

        # 一）没有持仓：开仓信号计算，进行开仓逻辑计算，主要依靠generate_open_signal方法，开仓成功后将会结束当前循环
        if current_position is None:
            print('当前无持仓, 计算开仓信号')

            # 计算开仓信号
            signal = generate_open_signal(current_timestamp_data)
            
            # 没信号就退出
            if signal is None:
                print('无开仓信号')
                continue

            current_position = {
                'symbol': signal['symbol'],
                'side': 'BUY',
                'sign_open_time': current_window_time,
                'sign_open_time_str': datetime.fromtimestamp(current_window_time/1000, tz=CHINA_TZ).strftime('%Y-%m-%d %H:%M:%S'),
            }
            print(f"发现做多开仓信号: {current_position['symbol']}")
            print(current_position)
            continue

        
        # 二）有持仓：平仓信号计算，计算当前持仓的盈亏（当前k就可以平仓，因为实盘是1h k线）
        else:
            # 当前信号数据
            current_symbol_row = current_timestamp_data[current_timestamp_data['symbol'] == current_position['symbol']].iloc[0]
            
            # 如果第一次出现信号，却没有开仓时间，说明需要主动赋值
            if current_position.get('trade_open_time') is None:
                # 计算开仓参数
                entry_price = float(current_symbol_row['open'])
                margin = cash_balance * 0.9
                quantity = margin / entry_price
                atr = float(current_symbol_row['atr'])

                current_position = {
                    # 开仓信号带来的
                    'symbol': current_position['symbol'],
                    'side': current_position['side'],
                    'sign_open_time': current_position['sign_open_time'],
                    'sign_open_time_str': current_position['sign_open_time_str'],
                    
                    # 回测循环计算的，开仓时间
                    'trade_open_time': current_window_time,
                    'trade_open_time_str': datetime.fromtimestamp(current_window_time/1000, tz=CHINA_TZ).strftime('%Y-%m-%d %H:%M:%S'),
                    
                    # 核心要计算的，开仓平仓价格
                    'entry_price': entry_price, # 固定 开仓价
                    
                    'history_highest_price': entry_price, # 会变化
                    
                    'exit_price': None, # 平仓价

                    # 购买
                    'margin': margin,
                    'quantity': quantity, # 不变
                }
                
                print(f"执行开仓: {current_position['symbol']} 时间: {current_position['trade_open_time_str']} 价格: {entry_price} 数量: {quantity}")

            # 计算平仓信号
            close_signal = generate_close_signal(current_position, current_symbol_row)

            # 出现平仓信号，说明本次交易已经结束
            if close_signal is not None:
                print(f"平仓信号: {close_signal['date_str']} 原因: {close_signal.get('reason', 'signal')}")
                
                # 确定平仓价格
                # 如果是固定止损(Low触发)，价格为止损价；如果是ATR(Close触发)，价格为收盘价
                entry_price = float(current_position['entry_price'])
                exit_price = float(close_signal.get('stop_price'))

                qty = float(current_position['quantity'])
                margin = float(current_position.get('margin', 100.0))

                entry_fee = entry_price * qty * 0.001
                exit_fee = exit_price * qty * 0.001
                profit = qty * (exit_price - entry_price) - entry_fee - exit_fee
                profit_pct = (profit / margin * 100.0) if margin != 0 else 0.0
                cash_balance = cash_balance + profit

                record = {
                    'symbol': current_position['symbol'],
                    'signal_open_time_str': current_position['sign_open_time_str'],
                    'trade_open_time_str': current_position['trade_open_time_str'],
                    'trade_close_time_str': close_signal['date_str'], # 当前K线内平仓

                    'signal_open_timestamp': int(current_position['sign_open_time']),
                    'trade_open_timestamp': int(current_position['trade_open_time']),
                    'trade_close_timestamp': close_signal['timestamp'],
                    
                    'entry_price': entry_price,
                    'exit_price': exit_price,

                    'atr': close_signal.get('atr'),

                    'profit': profit,
                    'profit_pct': profit_pct,
                    'final_balance': cash_balance,
                }

                trade_records_list.append(record)
                print(f"平仓: {current_position['symbol']} 价格: {exit_price:.6f} 盈亏: {profit:.6f} 当前余额: {cash_balance:.2f} USDT")
                current_position = None

                if cash_balance < 100.0:
                    print(f"资金低于 100 USDT，停止后续交易")
                    break
            # 没有平仓信号，则需要更新最高价，供下次平仓信号计算
            else:
                current_position['history_highest_price'] = max(float(current_symbol_row['high']), current_position['history_highest_price'])

    # 6. 保存结果到MongoDB
    trade_records = pd.DataFrame(trade_records_list)

    print(f"\n=== 回测完成 ===")
    print(f"总交易次数: {len(trade_records)}")
    print(f"最终余额: {cash_balance:.2f} USDT")
    print(f"总收益: {cash_balance - cash_balance_start:.2f} USDT")
    print(f"收益率: {((cash_balance - cash_balance_start) / cash_balance_start) * 100:.2f}%")
    
    print(f"\n批量插入 {len(trade_records)} 条交易信号记录")
    
    if not trade_records.empty:
        try:
            mongo_utils.insert_data('trade_records', trade_records)
            plot_backtest_results(trade_records, save_path='./record-md/account_curve.png', show_plot=True, initial_balance=cash_balance_start)
        except Exception as e:
            print(f"插入 trade_records 失败: {e}")

if __name__ == '__main__':
    main()
