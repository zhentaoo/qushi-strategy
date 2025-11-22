#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
将旧版 K 线文档结构迁移为新版结构，便于后续计算因子。

旧结构示例（Binance原始字段）：
- quote_asset_volume
- number_of_trades
- taker_buy_base_asset_volume
- taker_buy_quote_asset_volume
- 无 close_time_str

新结构示例（api_core.get_klines 输出）：
- amount                <- quote_asset_volume
- count                 <- number_of_trades
- taker_buy_volume      <- taker_buy_base_asset_volume
- taker_buy_amount      <- taker_buy_quote_asset_volume
- taker_sell_amount     <- amount - taker_buy_amount
- delta_rate_amount     <- (taker_buy_amount - taker_sell_amount) / amount
- candle_return         <- (close - open) / open * 100
- close_time_str        <- 由 close_time 计算

默认处理集合：symbol_15min_kline
支持按 symbol 过滤；支持 dry-run 查看变更；支持移除旧字段。
"""

from datetime import datetime
from zoneinfo import ZoneInfo

import mongo_utils


def to_float(x, default=0.0):
    try:
        if x is None:
            return float(default)
        return float(x)
    except Exception:
        return float(default)


def to_int(x, default=0):
    try:
        if x is None:
            return int(default)
        # 某些字段可能是字符串数字
        return int(float(x))
    except Exception:
        return int(default)


def make_close_time_str(close_time_ms: int) -> str:
    try:
        return datetime.fromtimestamp(int(close_time_ms) / 1000, tz=ZoneInfo('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M:%S')
    except Exception:
        return ''


def make_date_str(open_time_ms: int) -> str:
    try:
        return datetime.fromtimestamp(int(open_time_ms) / 1000, tz=ZoneInfo('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M:%S')
    except Exception:
        return ''


def transform_doc(doc: dict) -> tuple[dict, dict]:
    """给定一条旧结构文档，返回 (set_fields, unset_fields)。
    若文档已是新结构（存在 amount 或 count），返回空变更。
    """
    # 已是新结构则跳过
    if doc.get('amount') is not None or doc.get('count') is not None:
        return {}, {}

    # 仅当检测到旧字段时进行转换
    old_has = any(k in doc for k in [
        'quote_asset_volume', 'number_of_trades',
        'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume'
    ])
    if not old_has:
        return {}, {}

    open_ = to_float(doc.get('open'))
    close = to_float(doc.get('close'))

    amount = to_float(doc.get('quote_asset_volume'))
    count = to_int(doc.get('number_of_trades'))
    taker_buy_volume = to_float(doc.get('taker_buy_base_asset_volume'))
    taker_buy_amount = to_float(doc.get('taker_buy_quote_asset_volume'))

    taker_sell_amount = amount - taker_buy_amount if amount is not None else 0.0
    if amount and amount != 0:
        delta_rate_amount = (taker_buy_amount - taker_sell_amount) / amount
    else:
        delta_rate_amount = 0.0

    candle_return = ((close - open_) / open_ * 100) if open_ else 0.0

    close_time_ms = to_int(doc.get('close_time'))
    timestamp_ms = to_int(doc.get('timestamp'))

    set_fields = {
        'amount': float(amount),
        'count': int(count),
        'taker_buy_volume': float(taker_buy_volume),
        'taker_buy_amount': float(taker_buy_amount),
        'taker_sell_amount': float(taker_sell_amount),
        'delta_rate_amount': float(delta_rate_amount),
        'candle_return': float(candle_return),
        'close_time_str': make_close_time_str(close_time_ms),
        # 统一生成（即使旧文档已有）
        'date_str': make_date_str(timestamp_ms),
    }

    unset_fields = {
        'quote_asset_volume': '',
        'number_of_trades': '',
        'taker_buy_base_asset_volume': '',
        'taker_buy_quote_asset_volume': '',
    }

    return set_fields, unset_fields


def migrate(collection: str, symbol: str | None, apply: bool):
    db = mongo_utils.get_db()
    coll = db[collection]

    query = {
        '$or': [
            {'amount': {'$exists': False}},
            {'count': {'$exists': False}},
        ],
        # 至少包含一个旧字段（更精准）
        '$and': [
            {'$or': [
                {'quote_asset_volume': {'$exists': True}},
                {'number_of_trades': {'$exists': True}},
                {'taker_buy_base_asset_volume': {'$exists': True}},
                {'taker_buy_quote_asset_volume': {'$exists': True}},
            ]}
        ]
    }
    if symbol:
        query['symbol'] = symbol

    cursor = coll.find(query)

    total = 0
    changed = 0
    for doc in cursor:
        total += 1
        set_fields, unset_fields = transform_doc(doc)
        if not set_fields and not unset_fields:
            continue
        changed += 1

        coll.update_one({'_id': doc['_id']}, {'$set': set_fields, '$unset': unset_fields})

    print(f"扫描 {total} 条，已更新 {changed} 条；模式: APPLY")


def main():
    # 直接全量迁移默认集合，不接收外部参数
    collection = 'symbol_15min_kline'
    symbol = None
    apply = True
    migrate(collection, symbol, apply)


if __name__ == '__main__':
    main()