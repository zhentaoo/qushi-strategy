#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
大盘定时脚本（MongoDB版）：
- 每15分钟：不依赖24h接口；从交易所获取TRADING的USDT交易对并排除部分知名币对（如 BTC/ETH/SOL）；
- 对每个币对：首次拉取50根15m K线，后续只拉取最新1根并追加到MongoDB（避免重复，保证连续性）；
- 完成采集后：从MongoDB计算最新的市场季节；若为秋/冬则执行 s4 runtime。
"""

from typing import Any

from datetime import datetime
from concurrent.futures import ProcessPoolExecutor
from zoneinfo import ZoneInfo
import schedule
import pandas as pd

import api_core
import mongo_utils
import factor_utils

INTERVAL = '1h'

def get_candidate_symbols():
    """获取候选USDT交易对（始终通过交易所信息动态刷新）"""
    exchange_info = api_core.get_exchange_info()
    if not exchange_info:
        print("获取交易所信息失败")
        return []
    
    # 过滤：仅保留上线时间≥ 3个月
    now_ms = int(datetime.now().timestamp() * 1000)
    days_ms = 90 * 24 * 60 * 60 * 1000
    all_syms = [
        s.get('symbol')
        for s in exchange_info.get('symbols', [])
        if (
            s.get('status') == 'TRADING'
            and s.get('quoteAsset') == 'USDT'
            and (
                s.get('onboardDate') is not None
                and (now_ms - int(s.get('onboardDate'))) >= days_ms
            )
        )
    ]

    # 排除部分知名币对
    blacklist = {
        # 超主流币
        "BTCUSDT","ETHUSDT", "BNBUSDT", "SOLUSDT", 
        # 平台币
        "BNBUSDT", "OKBUSDT", "HTUSDT", "GTUSDT", "KCSUSDT", "LEOUSDT",
        # 稳定币/锚定资产（不应出现在策略中）
        "USDCUSDT", "FDUSDUSDT", "TUSDUSDT", "USDPUSDT", "DAIUSDT", "BUSDUSDT",
    }
    syms = [sym for sym in all_syms if sym not in blacklist]

    print(f"从交易所获取到有效 USDT 交易对: {len(all_syms)} 个，排除知名币对后: {len(syms)} 个")
    return syms


def _prepare_df_for_symbol(symbol: str) -> pd.DataFrame | None:
    """并发子任务：抓取该 symbol 最新 99 根K线（不做增量判断）"""
    limit = 99
    try:
        df = api_core.get_klines(symbol, interval=INTERVAL, limit=limit)
    except Exception as e:
        print(f"获取 {symbol} K线失败: {e}")
        return None

    if df is None or df.empty:
        print(f"{symbol} 无K线数据返回，跳过")
        return None

    # 保证时间升序
    df = df.sort_values('timestamp').reset_index(drop=True)

    print(f"✅ {symbol} 计划插入 {len(df)} 条K线")
    return df

def fetch_and_store_klines_for_symbols(symbols: list[str]):
    if not symbols:
        print("没有满足条件的交易对，跳过本轮")
        return

    """简化抓取逻辑：每次删除旧数据并为每个symbol抓取最新99根，统一并发后批量写入Mongo，保证线上数据一致性"""
    total_inserted = 0
    dfs_to_insert: list[pd.DataFrame] = []

    # 每次执行前，删除旧数据，避免重复与不连续
    mongo_utils.delete_data('runtime_symbol_1h_kline')

    # 固定抓取最新99根，不再查询latest_ts
    tasks: list[str] = []
    count = len(symbols)
    i = 1
    for symbol in symbols:
        print(f"共{count}个symbol，当前处理第{i}个symbol：{symbol}，limit=99")
        tasks.append(symbol)
        i += 1

    # 5个进程并发请求数据并收集待插入DataFrame
    with ProcessPoolExecutor(max_workers=3) as executor:
        for result in executor.map(_prepare_df_for_symbol, tasks):
            if result is not None and not result.empty:
                dfs_to_insert.append(result)

    # 统一批量插入
    if dfs_to_insert:
        batch_df = pd.concat(dfs_to_insert, ignore_index=True)
        inserted = mongo_utils.insert_data('runtime_symbol_1h_kline', batch_df)
        total_inserted += inserted
        print(f"📦 批量插入完成，共插入 {inserted} 条记录")
    else:
        print("无新增数据需要插入")

    print(f"本轮总插入条数: {total_inserted}")

def compute_factors() -> str | None:
    """从MongoDB取最近数据，计算market_season，返回最新时间点的季节"""
    # 每个币对取最近100条，足够计算移动均线
    df = mongo_utils.query_recent_data_by_symbol('runtime_symbol_1h_kline', limit_per_symbol=99)
    if df is None or df.empty:
        print("MongoDB中没有runtime_symbol_1h_kline数据")
        return None

    processed = factor_utils.compute_symbol_factor(df, is_runtime=False)

    # 将计算后，带因子的数据写入mongo，方便后续排查问题
    mongo_utils.delete_data('runtime_symbol_factor_1h_kline')
    mongo_utils.insert_data('runtime_symbol_factor_1h_kline', processed)

def main():
    print(f"=== {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}  所有币对1h数据抓取，因子计算 == ")

    # 1) 获取候选交易对（不调用24h行情）
    symbols = get_candidate_symbols()

    # 2) 拉取并写入MongoDB（首次99根，后续1根）
    fetch_and_store_klines_for_symbols(symbols)

    # 3) 计算因子
    compute_factors()

if __name__ == '__main__':
    print('run s_dapan.py')
    main()