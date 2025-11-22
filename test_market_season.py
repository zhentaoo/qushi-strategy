#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
测试大盘四季周期分析功能
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import factor_utils
import mongo_utils

def collect_real_data(limit_per_symbol=1000):
    """
    从MongoDB获取真实的15分钟K线数据
    """
    print("=== 从MongoDB获取15分钟K线数据 ===")
    
    # 从MongoDB获取15分钟K线历史数据，每个币对取最近500条数据
    kline_df = mongo_utils.query_recent_data_by_symbol('symbol_15min_kline', limit_per_symbol=limit_per_symbol)

    if kline_df.empty:
        print("MongoDB中没有symbol_15min_kline数据，请先运行get_all_data.py获取数据")
        return None
    
    print(f"从MongoDB获取到 {len(kline_df)} 条15分钟K线数据")
    print(f"数据包含 {kline_df['symbol'].nunique()} 个不同的币种")
    print(f"时间范围: {kline_df['timestamp'].nunique()} 个时间点")
    
    return kline_df

def test_market_season_analysis():
    """测试市场四季周期分析"""
    print("=== 测试大盘四季周期分析功能 ===")
    
    # 1. 从MongoDB获取真实数据
    print("1. 从MongoDB获取真实数据...")
    real_df = collect_real_data(limit_per_symbol=500)
    
    if real_df is None:
        print("❌ 无法获取数据，测试终止")
        return None
    
    print(f"   获取了 {len(real_df)} 行真实数据")
    print(f"   包含 {real_df['symbol'].nunique()} 个交易对")
    print(f"   时间范围: {real_df['timestamp'].nunique()} 个时间点")
    
    # 显示数据时间范围
    if len(real_df) > 0:
        min_time = pd.to_datetime(real_df['timestamp'], unit='ms').min()
        max_time = pd.to_datetime(real_df['timestamp'], unit='ms').max()
        print(f"   数据时间范围: {min_time} 到 {max_time}")
    
    # 2. 运行因子计算
    print("\n2. 运行因子计算...")
    result_df = factor_utils.compute_symbol_factor(real_df, is_runtime=False)
    mongo_utils.delete_data('test_market_season')
    mongo_utils.insert_data('test_market_season', result_df)
    
    # 3. 检查结果
    print("\n3. 检查计算结果...")
    
    # 检查新增的列
    new_columns = ['breadth_ratio', 'breadth_ma5', 'breadth_ma20', 'breadth_ma30', 'breadth_ma60', 'market_season']
    for col in new_columns:
        if col in result_df.columns:
            non_null_count = result_df[col].notna().sum()
            print(f"   ✓ {col}: {non_null_count}/{len(result_df)} 行有数据")
        else:
            print(f"   ✗ {col}: 列不存在")
    
    # 4. 显示市场季节分布
    print("\n4. 市场季节分布:")
    if 'market_season' in result_df.columns:
        season_counts = result_df['market_season'].value_counts()
        total_count = len(result_df[result_df['market_season'].notna()])
        for season, count in season_counts.items():
            percentage = (count / total_count * 100) if total_count > 0 else 0
            print(f"   {season}: {count} 次 ({percentage:.1f}%)")
    
    # 5. 显示最近几个时间点的详细数据
    print("\n5. 最近10个时间点的详细数据:")
    if len(result_df) > 0:
        # 获取最新的时间点数据（每个时间点选一个symbol）
        latest_data = result_df.groupby('timestamp').first().tail(10)
        
        display_cols = ['breadth_ratio', 'breadth_ma5', 'breadth_ma20', 'breadth_ma30', 'breadth_ma60', 'market_season']
        available_cols = [col for col in display_cols if col in latest_data.columns]
        
        if available_cols:
            # 添加时间列用于显示
            latest_data_display = latest_data.copy()
            latest_data_display['time'] = pd.to_datetime(latest_data_display.index, unit='ms').strftime('%m-%d %H:%M')
            display_cols_with_time = ['time'] + available_cols
            print(latest_data_display[display_cols_with_time].to_string())
        else:
            print("   没有可显示的列")
    
    # 6. 验证数据一致性
    print("\n6. 数据一致性验证:")
    
    # 检查breadth_ratio是否在合理范围内
    if 'breadth_ratio' in result_df.columns:
        breadth_data = result_df['breadth_ratio'].dropna()
        if len(breadth_data) > 0:
            min_val, max_val = breadth_data.min(), breadth_data.max()
            mean_val = breadth_data.mean()
            print(f"   breadth_ratio 范围: {min_val:.4f} - {max_val:.4f}, 平均值: {mean_val:.4f}")
            if 0 <= min_val <= max_val <= 1:
                print("   ✓ breadth_ratio 在合理范围内 [0, 1]")
            else:
                print("   ✗ breadth_ratio 超出合理范围")
    
    # 检查移动平均线的平滑性
    if all(col in result_df.columns for col in ['breadth_ma5', 'breadth_ma20', 'breadth_ma60']):
        ma_data = result_df[['breadth_ma5', 'breadth_ma20', 'breadth_ma60']].dropna()
        if len(ma_data) > 0:
            print(f"   移动平均线数据点: {len(ma_data)} 个")
            
            # 检查均线的合理性：短期均线应该更敏感
            ma5_std = ma_data['breadth_ma5'].std()
            ma60_std = ma_data['breadth_ma60'].std()
            print(f"   MA5标准差: {ma5_std:.4f}, MA60标准差: {ma60_std:.4f}")
            
            if ma5_std >= ma60_std:
                print("   ✓ 短期均线比长期均线更敏感（正常）")
            else:
                print("   ⚠️ 长期均线比短期均线更敏感（可能异常）")
    
    # 7. 季节变化趋势分析
    print("\n7. 季节变化趋势分析:")
    if 'market_season' in result_df.columns and len(result_df) > 0:
        # 按时间排序，查看最近的季节变化
        time_sorted = result_df.groupby('timestamp').first().sort_index()
        recent_seasons = time_sorted['market_season'].dropna().tail(20)
        
        if len(recent_seasons) > 0:
            print("   最近20个时间点的季节变化:")
            for timestamp, season in recent_seasons.items():
                time_str = pd.to_datetime(timestamp, unit='ms').strftime('%m-%d %H:%M')
                print(f"   {time_str}: {season}")
    
    print("\n=== 测试完成 ===")
    return result_df

if __name__ == '__main__':
    test_result = test_market_season_analysis()