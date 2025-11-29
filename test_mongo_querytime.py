#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
from zoneinfo import ZoneInfo
from datetime import datetime
import mongo_utils


def _to_ms(ts_str: str) -> int:
    dt = pd.to_datetime(ts_str)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=ZoneInfo('Asia/Shanghai'))
    return int(dt.timestamp() * 1000)


def run_case(collection: str, start_str: str, end_str: str, title: str):
    print(f"\n=== {title} ===")
    print(f"时间范围: {start_str} -> {end_str}")
    df = mongo_utils.query_data_by_timestamp(collection, start_str, end_str)
    if df is None or df.empty:
        print("未查询到数据")
        return

    print(f"数据形状: {df.shape}")
    symbols = df['symbol'].nunique() if 'symbol' in df.columns else 0
    print(f"币种数量: {symbols}")

    ts_min = int(df['timestamp'].min()) if 'timestamp' in df.columns else None
    ts_max = int(df['timestamp'].max()) if 'timestamp' in df.columns else None
    print(f"时间戳范围: {ts_min} 到 {ts_max}")

    is_sorted = bool(df['timestamp'].is_monotonic_increasing) if 'timestamp' in df.columns else True
    print(f"时间升序: {is_sorted}")

    # 校验范围包含
    start_ms = _to_ms(start_str)
    end_ms = _to_ms(end_str)
    # 不进行时间范围纠正，保持输入原样
    in_range = (ts_min is not None and ts_max is not None and ts_min >= start_ms and ts_max <= end_ms)
    print(f"范围覆盖正确: {in_range}")

    # 展示样例数据
    cols = [c for c in ['symbol', 'interval', 'date_str', 'timestamp', 'open', 'close', 'volume'] if c in df.columns]
    print("样例数据:")
    print(df[cols].head(3) if cols else df.head(3))


def main():
    collection = 'symbol_15min_kline'

    # 案例1：一天范围
    run_case(collection, '2025-10-01', '2025-10-02', '案例1：一天范围')

    # 案例3：一个月范围
    run_case(collection, '2025-10-01 00:00:00', '2025-11-01 00:00:00', '案例3：一个月范围')


if __name__ == '__main__':
    main()