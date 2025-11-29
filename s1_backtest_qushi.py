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

interval = '15m'  # 改为15分钟K线

# 生成时间窗口
def get_time_windows(df, window_min=15):
    """
    生成时间窗口，每15分钟一个窗口
    """
    if df.empty:
        return []
    
    # 获取数据的时间范围
    min_time = df['timestamp'].min()
    max_time = df['timestamp'].max()
    print(f"数据时间范围: {min_time} 到 {max_time}")
    
    
    # 生成4小时间隔的时间窗口
    window_ms = window_min * 60 * 1000  # 15分钟的毫秒数
    
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


    # 1. 从MongoDB获取15分钟K线数据
    start_time = time.time()
    kline_df = mongo_utils.query_data_by_timestamp('symbol_15min_kline', '2025-08-01', '2025-09-01')
    # kline_df = mongo_utils.query_data_by_timestamp('symbol_15min_kline', '2025-09-01', '2025-10-01')
    # kline_df = mongo_utils.query_data_by_timestamp('symbol_15min_kline', '2025-10-01', '2025-11-01')
    
    processed_df = factor_utils.compute_symbol_factor(kline_df)
    mongo_utils.insert_data('factor_processed_kline',processed_df)

    elapsed = time.time() - start_time
    print(f"K线数据长度: {len(kline_df)}")
    print(f"读取15分钟K线数据耗时: {elapsed:.2f}秒")


    # 2. 生成时间窗口
    time_windows = get_time_windows(processed_df, window_min=15)
    print(time_windows) 
    print(len(time_windows))

    # 3. 账户初始化回测参数
    cash_balance_start = 10000.0
    cash_balance = 10000.0
    trade_records = pd.DataFrame()
    current_position = None
    
    print(f"开始回测，初始资金: {cash_balance_start} USDT")
    print(f"时间窗口数量: {len(time_windows)}")
    
    # 4 回测循环
    for idx in range(1, len(time_windows) - 1):
        pre_window_time = time_windows[idx - 1]
        window_time = time_windows[idx]
        next_window_time = time_windows[idx + 1]

        # 当前时间窗口，所有币对的 指标/因子数据
        print(window_time)
        print(f"\n=== 时间窗口 {idx}/{len(time_windows)-1} ===")
        print(f"当前时间: {datetime.fromtimestamp(window_time/1000, tz=CHINA_TZ).strftime('%Y-%m-%d %H:%M:%S')}")

        # 开仓信号：没有持仓的时候，进行开仓逻辑计算，主要依靠generate_open_signal方法
        if current_position is None:
            print('当前无持仓, 计算开仓信号')
    
            # 计算因子数据
            print('开始计算指标')

            current_data = processed_df[processed_df['timestamp'] == window_time]
            signal = generate_open_signal(current_data)
            
            if signal is None:
                print('无开仓信号')
                continue

            if cash_balance < 1000:
                print(f"资金不足 ({cash_balance:.2f} USDT)，跳过开仓")
                continue

            entry_row = processed_df[(processed_df['symbol'] == signal['symbol']) & (processed_df['timestamp'] == next_window_time)]
            if entry_row is None or len(entry_row) == 0:
                continue

            entry_price = float(entry_row.iloc[0]['open'])
            quantity = cash_balance / entry_price

            current_position = {
                'symbol': signal['symbol'],
                'side':  signal['side'],
                'market_season': signal['market_season'],
                'roc_64': signal['roc_64'],
                'sign_open_time': window_time,
                'sign_open_time_str': datetime.fromtimestamp(window_time/1000, tz=CHINA_TZ).strftime('%Y-%m-%d %H:%M:%S'),
                'trade_open_time': next_window_time,
                'trade_open_time_str': datetime.fromtimestamp(next_window_time/1000, tz=CHINA_TZ).strftime('%Y-%m-%d %H:%M:%S'),
                'entry_price': entry_price,
                'quantity': quantity,
            }
            print(f"开仓: {current_position['symbol']} {('做多' if current_position['side']=='BUY' else '做空')} 价格: {entry_price:.6f} 数量: {quantity:.6f}")
            continue
        
        # 平仓信号：有持仓的时候，计算当前持仓的盈亏，如果在当前时间窗口盈利则继续持有，如果在当前时间窗口亏损 则平仓并计算盈亏
        elif current_position is not None:
            print('当前有持仓, 计算平仓信号')

            current_symbol_row_df = processed_df[(processed_df['symbol'] == current_position['symbol']) & (processed_df['timestamp'] == window_time)]
            if current_symbol_row_df is None or len(current_symbol_row_df) == 0:
                continue
            current_symbol_row = current_symbol_row_df.iloc[0]

            close_signal = generate_close_signal(current_position, current_symbol_row)
            
            
            if close_signal is not None:
                print(f"平仓信号: {close_signal['date_str']}")

                exit_row = processed_df[(processed_df['symbol'] == current_position['symbol']) & (processed_df['timestamp'] == next_window_time)]
                if exit_row is None or len(exit_row) == 0:
                    continue
                
                entry_price = float(current_position['entry_price'])
                exit_price = float(exit_row.iloc[0]['open'])

                qty = float(current_position['quantity'])

                profit = qty * (exit_price - entry_price) if current_position['side'] == 'BUY' else qty * (entry_price - exit_price)
                invested_notional = qty * entry_price
                profit_pct = (profit / invested_notional * 100) if invested_notional != 0 else 0.0
                cash_balance = cash_balance + profit

                record = {
                    'symbol': current_position['symbol'],
                    'signal_time_str': current_position['sign_open_time_str'],
                    'trade_open_time_str': current_position['trade_open_time_str'],
                    'sign_close_time_str': close_signal['date_str'],
                    'trade_close_time_str': datetime.fromtimestamp(next_window_time/1000, tz=CHINA_TZ).strftime('%Y-%m-%d %H:%M:%S'),

                    'signal_timestamp': int(current_position['sign_open_time']),
                    'trade_open_timestamp': int(current_position['trade_open_time']),
                    'sign_close_timestamp': close_signal['timestamp'],
                    'trade_close_timestamp': next_window_time,
                    
                    
                    'entry_price': entry_price,
                    'exit_price': exit_price,
                    'profit': profit,
                    'profit_pct': profit_pct,
                    'quantity': qty,
                    'final_balance': cash_balance,
                    'side': current_position['side'],
                    'market_season': current_position.get('market_season'),
                    'roc_64': current_position.get('roc_64'),
                }

                trade_records = pd.concat([trade_records, pd.DataFrame([record])], ignore_index=True)
                print(f"平仓: {current_position['symbol']} 价格: {exit_price:.6f} 盈亏: {profit:.6f} 当前余额: {cash_balance:.2f} USDT")
                current_position = None

    
    # 6. 保存结果到MongoDB
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