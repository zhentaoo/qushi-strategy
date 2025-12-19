#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import time
from datetime import datetime
from zoneinfo import ZoneInfo
import mongo_utils
import api_core
import argparse

def get_exchange_info():
    """获取交易所信息，筛选出有效的USDT交易对"""
    print("=== 获取交易所信息 ===")
    try:
        # 使用api_core中的方法获取交易所信息
        exchange_info = api_core.get_exchange_info()
        if not exchange_info:
            return None, None
        
        # 创建交易所信息的symbol映射，筛选status=TRADING且quoteAsset=USDT的交易对
        valid_symbols = {}
        filtered_symbols = []
        
        for symbol_info in exchange_info.get('symbols', []):
            symbol = symbol_info.get('symbol')
            status = symbol_info.get('status')
            quote_asset = symbol_info.get('quoteAsset')
            
            if status == 'TRADING' and quote_asset == 'USDT':
                valid_symbols[symbol] = symbol_info
                filtered_symbols.append(symbol_info)
        
        # 保存处理后的交易所信息到MongoDB
        processed_exchange_info = {
            'timestamp': int(time.time() * 1000),
            'serverTime': exchange_info.get('serverTime'),
            'symbols': filtered_symbols,
            'filtered_count': len(filtered_symbols)
        }
        mongo_utils.insert_data('exchange_info', processed_exchange_info)
        
        print(f"找到 {len(valid_symbols)} 个有效的USDT交易对")
        return valid_symbols, exchange_info
    except Exception as e:
        print(f"获取交易所信息失败: {e}")
        return None, None

import concurrent.futures

def process_symbol(symbol, symbol_info, start_ts, current_ts, interval, interval_ms, collection_name):
    """处理单个币种的数据抓取"""
    try:
        # 1. 确定该币种的抓取起始时间
        ts = start_ts
        
        # 获取上线时间，避免请求上线前的数据
        onboard_date = symbol_info.get('onboardDate')
        if onboard_date:
            ts = max(ts, int(onboard_date))
        
        # 查询数据库中该币种最新的K线时间
        db = mongo_utils.get_db()
        col = db[collection_name]
        last_record = col.find_one({'symbol': symbol}, sort=[('timestamp', -1)])
        
        if last_record:
            last_ts = last_record.get('timestamp')
            next_ts = last_ts + interval_ms
            ts = max(ts, next_ts)
        
        if ts >= current_ts:
            print(f"✅ {symbol} 数据已是最新")
            return 0

        print(f"🚀 {symbol} 开始抓取，起点: {pd.to_datetime(ts, unit='ms', utc=True).astimezone(ZoneInfo('Asia/Shanghai'))}")

        # 2. 循环抓取直到当前时间
        symbol_new_count = 0
        while ts < current_ts:
            try:
                limit = 900
                kline_data = api_core.get_klines(
                    symbol, interval=interval, limit=limit, startTime=ts
                )
                
                if kline_data is None or kline_data.empty:
                    print(f"⚠️ {symbol} 无返回数据")
                    break
                
                count = len(kline_data)
                mongo_utils.insert_data(collection_name, kline_data)
                symbol_new_count += count
                
                last_kline_ts = int(kline_data.iloc[-1]['timestamp'])
                ts = last_kline_ts + interval_ms
                
                # print(f"   -> {symbol} 获取 {count} 条，最新: {pd.to_datetime(last_kline_ts, unit='ms', utc=True).astimezone(ZoneInfo('Asia/Shanghai'))}")

                if count < limit:
                    break
                
                # 稍微休息一下，避免单个线程请求过快
                time.sleep(0.5)
                
            except Exception as e:
                print(f"❌ {symbol} 抓取出错: {e}")
                break
        
        if symbol_new_count > 0:
            print(f"✅ {symbol} 完成，新增 {symbol_new_count} 条")
        return symbol_new_count

    except Exception as e:
        print(f"❌ {symbol} 处理异常: {e}")
        return 0

def collect_kline_data(start_date_str='2025-01-01', interval='1h', max_workers=9):
    """
    收集K线数据
    :param start_date_str: 开始时间，格式 'YYYY-MM-DD'
    :param interval: K线间隔，如 '1h', '5m'
    """
    print(f"=== 开始收集 {interval} K线数据 ===", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    print(f"目标开始时间: {start_date_str}")

    # 1. 获取交易所信息和有效交易对
    valid_symbols, _ = get_exchange_info()
    if not valid_symbols:
        print("获取交易所信息失败，退出")
        return

    # 保存有效交易对列表
    symbol_list = list(valid_symbols.keys())
    valid_symbols_data = [{'symbol': symbol, 'timestamp': int(time.time() * 1000)} for symbol in symbol_list]
    mongo_utils.insert_data(f'symbol_{interval}_valid_symbols', valid_symbols_data)
    
    # 批量保存交易对详细信息
    symbol_details_list = []
    for symbol, symbol_info in valid_symbols.items():
        record = symbol_info.copy()
        record['timestamp'] = int(time.time() * 1000)
        symbol_details_list.append(record)
    
    if symbol_details_list:
        mongo_utils.insert_data('symbol_details', symbol_details_list)
    
    # 2. 准备时间参数
    interval_map = {
        '1m': 60 * 1000,
        '3m': 3 * 60 * 1000,
        '5m': 5 * 60 * 1000,
        '15m': 15 * 60 * 1000,
        '30m': 30 * 60 * 1000,
        '1h': 60 * 60 * 1000,
        '2h': 2 * 60 * 60 * 1000,
        '4h': 4 * 60 * 60 * 1000,
        '6h': 6 * 60 * 60 * 1000,
        '8h': 8 * 60 * 60 * 1000,
        '12h': 12 * 60 * 60 * 1000,
        '1d': 24 * 60 * 60 * 1000,
    }
    interval_ms = interval_map.get(interval)
    if not interval_ms:
        print(f"不支持的时间间隔: {interval}")
        return

    # 解析开始时间为毫秒时间戳 (默认视为北京时间)
    try:
        start_dt = pd.to_datetime(start_date_str)
        if start_dt.tzinfo is None:
            # 假设输入是北京时间
            start_dt = start_dt.replace(tzinfo=ZoneInfo('Asia/Shanghai'))
        start_ts = int(start_dt.timestamp() * 1000)
    except Exception as e:
        print(f"时间格式解析错误: {e}")
        return

    collection_name = f'symbol_{interval}_kline'
    db = mongo_utils.get_db()
    col = db[collection_name]
    
    # 创建索引
    try:
        col.create_index([('symbol', 1), ('timestamp', 1)], unique=True, background=True)
    except Exception:
        pass

    total_klines_count = 0
    print(f"开始获取 {len(valid_symbols)} 个交易对的数据...")
    
    # 获取当前时间戳作为统一的结束时间，避免不同币种抓取时间不一致
    current_ts = int(time.time() * 1000)
    
    print(f"使用 {max_workers} 个线程并发抓取...")
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for symbol, symbol_info in valid_symbols.items():
            futures.append(
                executor.submit(
                    process_symbol, 
                    symbol, 
                    symbol_info, 
                    start_ts, 
                    current_ts, 
                    interval, 
                    interval_ms, 
                    collection_name
                )
            )
        
        for future in concurrent.futures.as_completed(futures):
            try:
                count = future.result()
                total_klines_count += count
            except Exception as e:
                print(f"线程执行异常: {e}")

    print(f"=== {interval} 数据收集完成，总计新增 {total_klines_count} 条 ===", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

if __name__ == "__main__":
    collect_kline_data(start_date_str='2025-01-01', interval='1h', max_workers=1)
