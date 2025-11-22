#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import mongo_utils

def test_pagination():
    """测试分页功能"""
    
    print("=== 测试MongoDB分页查询功能 ===\n")
    
    # 测试1: 获取每个币种最新的1000条数据
    print("1. 获取每个币种最新的1000条数据:")
    df1 = mongo_utils.query_recent_data_by_symbol('symbol_15min_kline', limit_per_symbol=1000)
    if not df1.empty:
        print(f"   数据形状: {df1.shape}")
        print(f"   币种数量: {df1['symbol'].nunique()}")
        print(f"   时间范围: {df1['timestamp'].min()} 到 {df1['timestamp'].max()}")
        print(df1)
    
    # 测试2: 获取每个币种第1001-2000条数据（分页）
    print("2. 获取每个币种第1001-2000条数据（分页）:")
    df2 = mongo_utils.query_recent_data_by_symbol('symbol_15min_kline', 
                                                  limit_per_symbol=1000, 
                                                  skip_per_symbol=1000)
    if not df2.empty:
        print(f"   数据形状: {df2.shape}")
        print(f"   币种数量: {df2['symbol'].nunique()}")
        print(f"   时间范围: {df2['timestamp'].min()} 到 {df2['timestamp'].max()}")
        print()
    
    # 测试3: 获取每个币种第2001-3000条数据
    print("3. 获取每个币种第2001-3000条数据:")
    df3 = mongo_utils.query_recent_data_by_symbol('symbol_15min_kline', 
                                                  limit_per_symbol=1000, 
                                                  skip_per_symbol=2000)
    if not df3.empty:
        print(f"   数据形状: {df3.shape}")
        print(f"   币种数量: {df3['symbol'].nunique()}")
        print(f"   时间范围: {df3['timestamp'].min()} 到 {df3['timestamp'].max()}")
        print()
    
    # 测试4: 小批量测试，每个币种取5条数据
    print("4. 小批量测试 - 每个币种最新5条数据:")
    df4 = mongo_utils.query_recent_data_by_symbol('symbol_15min_kline', limit_per_symbol=5)
    if not df4.empty:
        print(f"   数据形状: {df4.shape}")
        print(f"   币种数量: {df4['symbol'].nunique()}")
        print("   各币种数据条数:")
        symbol_counts = df4['symbol'].value_counts()
        for symbol, count in symbol_counts.head(10).items():
            print(f"     {symbol}: {count}条")
        print()
    
    # 测试5: 验证分页的时间连续性（以BTCUSDT为例）
    if not df1.empty and not df2.empty:
        print("5. 验证分页时间连续性（以BTCUSDT为例）:")
        btc_df1 = df1[df1['symbol'] == 'BTCUSDT'].sort_values('timestamp', ascending=False)
        btc_df2 = df2[df2['symbol'] == 'BTCUSDT'].sort_values('timestamp', ascending=False)
        
        if not btc_df1.empty and not btc_df2.empty:
            print(f"   第一页BTCUSDT最新时间: {btc_df1.iloc[0]['timestamp']}")
            print(f"   第一页BTCUSDT最旧时间: {btc_df1.iloc[-1]['timestamp']}")
            print(f"   第二页BTCUSDT最新时间: {btc_df2.iloc[0]['timestamp']}")
            print(f"   第二页BTCUSDT最旧时间: {btc_df2.iloc[-1]['timestamp']}")
            
            # 验证第二页的最新时间应该早于第一页的最旧时间
            page1_oldest = btc_df1.iloc[-1]['timestamp']
            page2_newest = btc_df2.iloc[0]['timestamp']
            print(f"   时间连续性检查: {'✓ 正确' if page2_newest < page1_oldest else '✗ 错误'}")

if __name__ == '__main__':
    test_pagination()