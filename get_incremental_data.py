#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import time
import os
import pandas as pd
from datetime import datetime
from zoneinfo import ZoneInfo
from concurrent.futures import ThreadPoolExecutor, as_completed

import mongo_utils
import api_core

INTERVAL = '15m'
INTERVAL_MS = 15 * 60 * 1000  # 15分钟 = 900,000ms
MAX_WORKERS = 4  # 并发线程数，可根据API限流调整


def get_existing_symbols():
    """从MongoDB现有表中获取需要增量更新的symbol列表"""
    db = mongo_utils.get_db()
    col = db['symbol_15min_kline']
    try:
        symbols = sorted(col.distinct('symbol'))
        print(f"发现 {len(symbols)} 个需要更新的交易对")
        return symbols
    except Exception as e:
        print(f"获取symbol列表失败: {e}")
        return []


def get_latest_ts(symbol: str) -> int | None:
    """查询某个symbol在symbol_15min_kline中的最新timestamp（毫秒）"""
    db = mongo_utils.get_db()
    col = db['symbol_15min_kline']
    try:
        cursor = col.find({'symbol': symbol}).sort('timestamp', -1).limit(1)
        docs = list(cursor)
        if not docs:
            return None
        ts = int(docs[0].get('timestamp'))
        return ts
    except Exception as e:
        print(f"查询 {symbol} 最新timestamp失败: {e}")
        return None


def fetch_incremental_klines(symbol: str, start_ts: int, end_ts: int) -> pd.DataFrame:
    """从start_ts到end_ts增量拉取15m K线，按批次（最多1000根/批）获取并拼接"""
    all_batches = []
    cur_start = start_ts

    # 防止请求到未来未完成K线，api_core.get_klines内部也做了过滤
    while cur_start < end_ts:
        batch_end = min(cur_start + 1000 * INTERVAL_MS - 1, end_ts)
        try:
            df = api_core.get_klines(
                symbol,
                interval=INTERVAL,
                limit=1000,
                startTime=cur_start,
                endTime=batch_end,
            )
        except Exception as e:
            print(f"获取 {symbol} 增量K线失败: {e}")
            break

        if df is None or df.empty:
            # 没有新数据，提前结束
            break

        # 保证时间顺序一致
        df = df.sort_values('timestamp')
        all_batches.append(df)

        last_ts = int(df['timestamp'].max())
        # 推进起点，避免重复
        if last_ts + 1 <= cur_start:
            # 若没有推进，避免死循环
            break
        cur_start = last_ts + 1


    if all_batches:
        return pd.concat(all_batches, ignore_index=True)
    else:
        return pd.DataFrame()


def main():
    print("=== 开始增量获取15分钟K线数据 ===", datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M:%S'))

    symbols = get_existing_symbols()
    if not symbols:
        print("symbol_15min_kline表为空，请先运行get_all_data.py初始化全量数据")
        return

    now_ms = int(time.time() * 1000)
    total_new = 0

    print(f"准备并发更新 {len(symbols)} 个交易对，线程数: {MAX_WORKERS}")

    def process_symbol(symbol: str, end_ts: int) -> int:
        try:
            latest_ts = get_latest_ts(symbol)
            if latest_ts is None:
                print(f"{symbol} 无历史记录，跳过")
                return 0

            start_ts = latest_ts + 1  # 从最新记录后的下一毫秒开始，避免重复

            latest_str = pd.to_datetime(latest_ts, unit='ms').tz_localize(ZoneInfo('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M:%S')
            start_str = pd.to_datetime(start_ts, unit='ms').tz_localize(ZoneInfo('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M:%S')
            end_str = pd.to_datetime(end_ts, unit='ms').tz_localize(ZoneInfo('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M:%S')
            print(f"{symbol} 最新时间: {latest_str}，增量范围: {start_str} -> {end_str}")

            if start_ts >= end_ts:
                print(f"{symbol} 无需更新（已是最新）")
                return 0

            df_new = fetch_incremental_klines(symbol, start_ts, end_ts)
            if df_new is None or df_new.empty:
                print(f"{symbol} 本次无新增K线")
                return 0

            # 写入MongoDB（不删除或修改既有数据）
            inserted = mongo_utils.insert_data('symbol_15min_kline', df_new)
            print(f"✅ {symbol} 插入 {inserted} 条新增K线")
            return inserted
        except Exception as e:
            print(f"处理 {symbol} 失败: {e}")
            return 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_symbol = {executor.submit(process_symbol, symbol, now_ms): symbol for symbol in symbols}
        for future in as_completed(future_to_symbol):
            inserted = future.result()
            total_new += inserted

    print(f"=== 增量更新完成，新增 {total_new} 条记录 ===", datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M:%S'))

if __name__ == '__main__':
    main()
