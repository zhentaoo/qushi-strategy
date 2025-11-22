#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
从 MongoDB 的 runtime_symbol_factor_15min_kline 集合读取指定 timestamp 的所有数据，
并按 roc_64 从大到小排序，打印并保存到 CSV。

使用示例：
    python test.dapan.py 1762614000000
不传参则默认使用 1762614000000。
"""

import sys
import pandas as pd
import pymongo
import mongo_utils


def main():
    # 读取命令行参数中的 timestamp，默认 1762614000000
    try:
        target_ts = int(sys.argv[1]) if len(sys.argv) > 1 else 1762614000000
    except ValueError:
        print("timestamp 参数无效，应为毫秒级整数")
        return

    print(f"查询集合 runtime_symbol_factor_15min_kline，timestamp = {target_ts}")

    # 连接 MongoDB
    db = mongo_utils.get_db()
    col = db['runtime_symbol_factor_15min_kline']

    # 查询并按 roc_64 降序排序（同时过滤掉不存在或为 null 的 roc_64）
    cursor = (
        col.find({
            'timestamp': target_ts,
            'roc_64': { '$exists': True, '$ne': None }
        })
        .sort('roc_64', pymongo.DESCENDING)
    )

    docs = list(cursor)
    if not docs:
        print("未查询到数据或该时间点无有效的 roc_64 值")
        return

    df = pd.DataFrame(docs)

    # 保险起见，将 roc_64 转为数值再排序
    df['roc_64'] = pd.to_numeric(df['roc_64'], errors='coerce')
    df = df.dropna(subset=['roc_64']).sort_values('roc_64', ascending=False).reset_index(drop=True)

    # 选择常见关键信息列，若不存在则自动忽略
    preferred_cols = ['symbol', 'roc_64', 'timestamp', 'interval', 'date_str', 'open', 'high', 'low', 'close', 'volume']
    cols = [c for c in preferred_cols if c in df.columns]
    if cols:
        view = df[cols]
    else:
        view = df

    # 打印前 30 行作为预览
    print("\n按 roc_64 降序排序的前 30 条：")
    print(view.head(30).to_string(index=False))

    # 保存到 CSV
    out_path = f"runtime_sorted_roc64_{target_ts}.csv"
    view.to_csv(out_path, index=False)
    print(f"\n已保存排序结果到: {out_path}")


if __name__ == '__main__':
    main()