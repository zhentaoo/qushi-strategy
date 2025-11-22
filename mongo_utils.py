#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pymongo
import pandas as pd
from datetime import datetime


# MongoDB数据库配置
MONGO_HOST = 'localhost'
MONGO_PORT = 27017
MONGO_DATABASE = 'bian'
MONGO_USER = 'admin'
MONGO_PASSWORD = 'wwwxxxdskjkl123990'  # 替换成实际密码

def get_db():
    """获取数据库连接"""
    client = pymongo.MongoClient(MONGO_HOST, MONGO_PORT, username=MONGO_USER, password=MONGO_PASSWORD)
    return client[MONGO_DATABASE]

def insert_data(collection_name, data):
    db = get_db()
    collection = db[collection_name]
    
    # 处理不同类型的数据
    if isinstance(data, pd.DataFrame):
        records = data.to_dict('records')
    elif isinstance(data, dict):
        records = [data]
    elif isinstance(data, list):
        records = data
    else:
        records = [{'data': data, 'timestamp': datetime.now()}]
    
    if records:
        result = collection.insert_many(records)
        print(f"插入数据到 {collection_name}: {len(records)} 条记录")
        return len(records)
    else:
        print(f"没有数据插入到 {collection_name}")
        return 0

def query_data(collection_name):
    """批量查询集合中的所有数据"""
    db = get_db()
    collection = db[collection_name]
    
    cursor = collection.find({})
    records = list(cursor)
    
    if records:
        df = pd.DataFrame(records)
        print(f"从 {collection_name} 查询到 {len(df)} 条记录")
        return df
    else:
        print(f"从 {collection_name} 未查询到数据")
        return pd.DataFrame()

# 分页查询数据库里的所有symbol
def query_recent_data_by_symbol(collection_name, limit_per_symbol=1000, skip_per_symbol=0):
    db = get_db()
    collection = db[collection_name]

    try:
        # 获取所有 symbol（一次查询即可）
        symbols = collection.distinct('symbol')
        if not symbols:
            print(f"从 {collection_name} 未查询到数据")
            return pd.DataFrame()

        # 提前建立索引（如果没有的话）
        collection.create_index([("symbol", 1), ("timestamp", -1)], background=True)

        records = []
        for symbol in symbols:
            try:
                cursor = (
                    collection.find({'symbol': symbol}, projection={'_id': 0})
                    .sort('timestamp', -1)
                    .skip(int(skip_per_symbol))
                    .limit(int(limit_per_symbol))
                )
                records.extend(cursor)
            except Exception as e:
                print(f"查询币种 {symbol} 失败: {e}")
                continue

        if not records:
            print(f"从 {collection_name} 未查询到数据")
            return pd.DataFrame()

        df = pd.DataFrame(records)
        symbols_count = len(symbols)
        page_info = f"跳过{skip_per_symbol}条" if skip_per_symbol > 0 else "最新"
        print(f"从 {collection_name} 查询到 {len(df)} 条记录，包含 {symbols_count} 个币种，每个币种{page_info}的 {limit_per_symbol} 条数据")
        return df

    except Exception as e:
        print(f"查询 {collection_name} 失败: {e}")
        return pd.DataFrame()

def delete_data(collection_name):
    """批量删除集合中的所有数据"""
    db = get_db()
    collection = db[collection_name]
    
    result = collection.delete_many({})
    deleted_count = result.deleted_count
    
    print(f"清空 {collection_name} 集合，删除 {deleted_count} 条记录")
    return deleted_count

