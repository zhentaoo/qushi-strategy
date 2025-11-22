#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
import time
from datetime import datetime, timedelta, timezone
import pytz
import mongo_utils
import api_core
import factor_utils
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

def collect_all_data():
    """收集所有数据的主函数"""
    print("=== 开始收集数据 ===", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    
    # 清空旧数据
    print("清空旧数据")
    mongo_utils.delete_data('coins')
    mongo_utils.delete_data('coin_history')
    
    # 1. 获取交易所信息和有效交易对
    valid_symbols, _ = get_exchange_info()
    if not valid_symbols:
        print("获取交易所信息失败，退出")
        return
    
    # 2. 获取所有有效交易对的5分钟K线数据
    all_coin_history_5m = pd.DataFrame()
    coins_to_save = []
    
    print(f"开始获取 {len(valid_symbols)} 个交易对的5分钟K线数据...")
    
    for i, symbol in enumerate(valid_symbols.keys(), 1):
        print(f"处理进度: {i}/{len(valid_symbols)} - {symbol}")
        
        # 获取1400根5分钟K线
        kline_data = get_kline_data(symbol, interval='5m', limit=1400)
        if not kline_data:
            continue
        
        # 处理5分钟K线数据
        if symbol_df_5m is None:
            continue
        
        # 合并5分钟K线数据
        all_coin_history_5m = pd.concat([all_coin_history_5m, symbol_df_5m], ignore_index=True)
        
        # 保存5分钟K线到MongoDB
        mongo_utils.insert_data('coin_history', symbol_df_5m)
        print(f"成功获取并保存 {symbol} 的 {len(symbol_df_5m)} 条5分钟K线数据")
        
        # 准备保存币种信息
        coins_to_save.append({
            'symbol': symbol,
            'priceChangePercent': 0,  # 默认值
            'ts': int(time.time() * 1000),
            'date_str': datetime.now(tz=timezone(timedelta(hours=8))).strftime('%Y-%m-%d %H:%M:%S'),
            'interval': '5m'
        })
        
        # 每处理50个币种休息一下，避免API限制
        if i % 50 == 0:
            print(f"已处理 {i} 个币种，休息2秒...")
            time.sleep(2)
    
    # 保存币种数据
    if coins_to_save:
        df = pd.DataFrame(coins_to_save)
        mongo_utils.insert_data('coins', df)
        print(f"保存 {len(df)} 个币种数据")
    
    print(f"总共收集 {len(all_coin_history_5m)} 条5分钟K线数据")
    print("=== 数据收集完成 ===", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

def collect_15min_kline_data():
    # 1. 获取交易所信息和有效交易对
    mongo_utils.delete_data('symbol_15min_valid_symbols')

    valid_symbols, _ = get_exchange_info()
    if not valid_symbols:
        print("获取交易所信息失败，退出")
        return

    # 保存有效交易对列表到symbol_15min_valid_symbols集合
    symbol_list = list(valid_symbols.keys())
    valid_symbols_data = [{'symbol': symbol, 'timestamp': int(time.time() * 1000)} for symbol in symbol_list]
    mongo_utils.insert_data('symbol_15min_valid_symbols', valid_symbols_data)
    
    # 批量保存交易对详细信息到MongoDB
    symbol_details_list = []
    for symbol, symbol_info in valid_symbols.items():
        # 创建新的字典，避免修改原始数据
        record = {}
        for key, value in symbol_info.items():
            record[key] = value
        record['timestamp'] = int(time.time() * 1000)
        symbol_details_list.append(record)
    
    # 一次性批量插入所有交易对详细信息
    if symbol_details_list:
        result = mongo_utils.insert_data('symbol_details', symbol_details_list)
        print(f"批量插入 {len(symbol_details_list)} 个交易对详细信息到MongoDB，实际插入: {result} 条记录")
    
    # 清空旧数据
    print("清空旧数据")
    mongo_utils.delete_data('symbol_15min_kline')
    
    # 2. 获取所有有效交易对的15分钟K线数据
    total_klines_count = 0
    
    print(f"开始获取 {len(valid_symbols)} 个交易对的15分钟K线数据...")
    
    for i, symbol in enumerate(valid_symbols.keys(), 1):
        print(f"处理进度: {i}/{len(valid_symbols)} - {symbol}")
        
        # 用于存储该币对的所有K线数据（DataFrame列表）
        all_symbol_klines = [] 
        
        db = mongo_utils.get_db()
        col = db['symbol_15min_kline']
        try:
            col.create_index([('symbol', 1), ('timestamp', 1)], background=True)
        except Exception:
            pass

        # 15分钟K线的时间间隔（毫秒）
        interval_ms = 15 * 60 * 1000  # 15分钟 = 900,000毫秒
        
        # 获取当前时间戳作为结束时间
        current_time = int(time.time() * 1000)
        
        # 分5次获取，每次1000条，总共最多5000条
        for batch in range(30):
            if batch == 0:
                continue
            try:
                # 计算这一批的结束时间和开始时间
                end_time = current_time - (batch * 1000 * interval_ms)
                start_time = end_time - (1000 * interval_ms)
                
                print(f"📊 {symbol} 批次 {batch+1}/5 时间范围: {pd.to_datetime(start_time, unit='ms')} 到 {pd.to_datetime(end_time, unit='ms')}")
                
                try:
                    exists = col.find_one({'symbol': symbol, 'timestamp': {'$gte': start_time, '$lte': end_time}})
                except Exception:
                    exists = None
                if exists:
                    print(f"⏭️ {symbol} 批次 {batch+1}/5 数据已存在，跳过")
                    time.sleep(0.05)
                    continue

                # 获取15分钟K线数据
                kline_data = api_core.get_klines(
                    symbol, 
                    interval='15m', 
                    limit=1000,
                    startTime=start_time,
                    endTime=end_time
                )
                
                # 兼容空返回（DataFrame或None）
                if kline_data is None or (isinstance(kline_data, pd.DataFrame) and kline_data.empty):
                    print(f"⚠️ {symbol} 批次 {batch+1}/5 没有获取到K线数据，跳过后续批次")
                    break
                
                # 累积DataFrame
                if isinstance(kline_data, pd.DataFrame):
                    all_symbol_klines.append(kline_data)
                else:
                    # 兜底：若返回为列表，则转换为DataFrame
                    all_symbol_klines.append(pd.DataFrame(kline_data))
                
                print(f"📊 {symbol} 批次 {batch+1}/5 获取到 {len(kline_data)} 根K线")
                
                # 如果获取的数据少于1000条，说明已经没有更多数据了
                if len(kline_data) < 999:
                    print(f"⚠️ {symbol} 批次 {batch+1}/5 获取到的K线数据少于1000条，跳过后续批次")
                    break
                
                # 每次请求后休息一下，避免API限制
                time.sleep(0.2)
                
            except Exception as e:
                print(f"获取 {symbol} 批次 {batch+1}/5 K线数据失败: {e}")
                # 出错后休息一下再继续
                time.sleep(1)
                continue
        
        # 处理获取到的所有K线数据
        if all_symbol_klines:
            # 合并并按时间戳排序（从旧到新）
            df = pd.concat(all_symbol_klines, ignore_index=True)
            if 'timestamp' in df.columns:
                df = df.sort_values('timestamp')
            
            # 保存到MongoDB
            mongo_utils.insert_data('symbol_15min_kline', df)
            
            total_klines_count += len(df)
            print(f"✅ 成功获取并保存 {symbol} 的 {len(df)} 条15分钟K线数据")
        
        # 每处理10个币种休息一下，避免API限制
        if i % 10 == 0:
            print(f"已处理 {i} 个币种，休息3秒...")
            time.sleep(3)
    
    print(f"总共收集 {total_klines_count} 条15分钟K线数据")
    print("=== 15分钟K线数据收集完成 ===", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))


if __name__ == "__main__":
    collect_15min_kline_data()
