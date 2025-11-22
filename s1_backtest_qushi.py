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
from s1_strategy import generate_signal

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
    
    # 生成4小时间隔的时间窗口
    window_ms = window_min * 60 * 1000  # 15分钟的毫秒数
    
    windows = []
    current_time = min_time
    while current_time <= max_time:
        windows.append(current_time)
        current_time += window_ms
    
    print(f"生成 {len(windows)} 个时间窗口")
    return windows

# 交易执行：多、空
def execute_trade_single_kline(symbol, timestamp, history_df, cash_balance, side):
    """
    在单个K线内完成交易：open价格开仓，close价格平仓
    """
    time_col = 'timestamp' if 'timestamp' in history_df.columns else ('open_time' if 'open_time' in history_df.columns else None)
    if time_col is None:
        return None, cash_balance
    data = history_df[(history_df['symbol'] == symbol) & (history_df[time_col] == timestamp)]
    if data.empty:
        return None, cash_balance
    
    if cash_balance < 10:
        print(f"资金不足 ({cash_balance:.2f} USDT)，跳过 {symbol} 交易")
        return None, cash_balance

    max_trade_amount = 20000
    if cash_balance > max_trade_amount:
        trade_amount = max_trade_amount
    else:
        trade_amount = cash_balance

    open_val = data.iloc[0]['open']
    close_val = data.iloc[0]['close']

    if pd.isna(open_val) or pd.isna(close_val):
        return None, cash_balance

    open_price = float(open_val)
    close_price = float(close_val)

    fee_rate = 0.0004
    entry_price = open_price
    exit_price = close_price

    if side == 'BUY': # 做多：open买入，close卖出
        price_return = (close_price - open_price) / open_price
    
    if side == 'SELL': # 做空：open卖出，close买入
        price_return = -(close_price - open_price) / open_price

    # 计算交易费用（开仓和平仓各收一次）
    total_fee_rate = fee_rate * 2
    final_trade_amount = trade_amount * (1 + price_return - total_fee_rate)
    profit = final_trade_amount - trade_amount
    profit_pct = (profit / trade_amount) * 100
    quantity = trade_amount / entry_price
    new_balance = cash_balance + profit

    trade = {
        'timestamp': int(timestamp) if timestamp is not None else None,
        'date': datetime.fromtimestamp(int(timestamp)/1000, tz=CHINA_TZ).strftime('%Y-%m-%d %H:%M:%S') if timestamp is not None else None,
        'symbol': str(symbol),
        'entry_price': float(entry_price),
        'exit_price': float(exit_price),
        'side': '做多' if side == 1 else '做空',
        'quantity': float(quantity),
        'profit': float(profit),
        'profit_pct': float(profit_pct),
        'final_balance': float(new_balance)
    }

    return trade, float(new_balance)

# 绘图与统计
def plot_backtest_results(trades_data, save_path='./record-md/account_curve.png', show_plot=True, initial_balance=1000):
    print("开始绘制交易收益曲线")
    
    if isinstance(trades_data, list):
        df = pd.DataFrame(trades_data)
    else:
        df = trades_data.copy()
    
    if df is None or df.empty:
        return None
    
    df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms')
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

    initial_balance = df['cumulative_balance'].iloc[0] - df['profit'].iloc[0]
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
    # 0. 清空trading_signals
    mongo_utils.delete_data('trading_signals')

    # 1. 从MongoDB获取15分钟K线数据
    start_time = time.time()
    kline_df = mongo_utils.query_recent_data_by_symbol('symbol_15min_kline', limit_per_symbol=1000)
    elapsed = time.time() - start_time
    print(f"读取15分钟K线数据耗时: {elapsed:.2f}秒")

    # 2. 计算因子数据
    print('开始计算指标')
    processed_df = factor_utils.compute_symbol_factor(kline_df)
    # mongo_utils.delete_data('factor_processed_kline')
    # mongo_utils.insert_data('factor_processed_kline',processed_df)

    # 3. 生成时间窗口
    time_windows = get_time_windows(processed_df, window_min=15)
    print(time_windows) 

    # 4. 初始化回测参数
    cash_balance_start = 1000.0
    cash_balance = 1000.0
    trades = []
    signal_records = []
    
    print(f"开始回测，初始资金: {cash_balance_start} USDT")
    print(f"时间窗口数量: {len(time_windows)}")
    
    # 5. 时间循环回测
    for i, window_time in enumerate(time_windows[:-1]):  # 排除最后一个窗口
        next_window_time = time_windows[i + 1]
        
        print(f"\n=== 时间窗口 {i+1}/{len(time_windows)-1} ===")
        print(window_time)
        print(f"当前时间: {datetime.fromtimestamp(window_time/1000, tz=CHINA_TZ).strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"下个时间: {datetime.fromtimestamp(next_window_time/1000, tz=CHINA_TZ).strftime('%Y-%m-%d %H:%M:%S')}")

        # 5.1 生成做空信号
        current_data = processed_df[processed_df['timestamp'] == window_time]
        signal = generate_signal(current_data)

        if signal is None:
            continue

        symbol = signal['symbol']
        side = signal['side']

        print(f"选择{side}信号: {symbol} (总分: {signal['score']:.2f})")
        print(f"  详细指标:")
        candle_desc = '阳线' if signal.get('close') > signal.get('open') else ('阴线' if signal.get('close') < signal.get('open') else '十字')
        print(f"    market_season: {signal['market_season']}")
        print(f"    ATR激增评分: {signal['score']:.2f}")
        print(f"    candle: {candle_desc}")

        # 在下一个时间窗口的K线上执行交易（open做空，close平仓）
        trade, cash_balance = execute_trade_single_kline(symbol, next_window_time, processed_df, cash_balance, side)

        if trade is not None:
            trades.append(trade)
            print(f"执行{side}交易: {symbol}, 收益: {trade['profit']:.2f} USDT ({trade['profit_pct']:.2f}%), 余额: {cash_balance:.2f} USDT")
            
            # 记录交易信号
            signal_record = {
                'symbol': str(symbol),
                'signal_timestamp': int(window_time),
                'signal_datetime': datetime.fromtimestamp(window_time/1000, tz=CHINA_TZ).strftime('%Y-%m-%d %H:%M:%S'),
                'trade_timestamp': int(next_window_time),
                'trade_datetime': datetime.fromtimestamp(next_window_time/1000, tz=CHINA_TZ).strftime('%Y-%m-%d %H:%M:%S'),
                'market_season': str(signal['market_season']),
                'roc_64': signal['roc_64'],
                'score': float(signal['score']),
                'profit': trade['profit'],
                'profit_pct': trade['profit_pct'],
                'balance': trade['final_balance'],
                'created_at': datetime.now(tz=CHINA_TZ)
            }
            signal_records.append(signal_record)

    
    # 6. 保存结果到MongoDB
    print(f"\n=== 回测完成 ===")
    print(f"总交易次数: {len(trades)}")
    print(f"最终余额: {cash_balance:.2f} USDT")
    print(f"总收益: {cash_balance - cash_balance_start:.2f} USDT")
    print(f"收益率: {((cash_balance - cash_balance_start) / cash_balance_start) * 100:.2f}%")
    
    print(f"\n批量插入 {len(signal_records)} 条交易信号记录")
    
    if signal_records:
        try:
            mongo_utils.insert_data('trading_signals', signal_records)
        except Exception as e:
            print(f"插入 trading_signals 失败: {e}")

    
    # 7. 绘制回测结果
    if trades:
        plot_backtest_results(trades, save_path='./record-md/account_curve.png', show_plot=True, initial_balance=cash_balance_start)
    else:
        print("无满足条件的做空信号")

if __name__ == '__main__':
    main()