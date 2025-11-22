#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Delta因子与价格相关性分析脚本

分析delta_rate（主动买入占总成交比）与下一个K线涨跌幅的相关性
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats
from scipy.stats import pearsonr, spearmanr
import warnings
warnings.filterwarnings('ignore')

# 导入本地模块
from mongo_utils import query_data

# 设置中文字体
plt.rcParams['font.sans-serif'] = ['SimHei', 'Arial Unicode MS', 'DejaVu Sans']
plt.rcParams['axes.unicode_minus'] = False

def load_data(symbol='COAIUSDT', interval='1m', limit=None):
    """
    从MongoDB加载指定交易对和时间周期的数据
    
    Args:
        symbol: 交易对名称，默认为COAIUSDT
        interval: 时间周期，默认为1m
        limit: 限制数据条数，None表示不限制
    
    Returns:
        DataFrame: 包含历史数据的DataFrame
    """
    collection_name = f"{symbol}_{interval}_processed"
    print(f"正在从MongoDB加载{collection_name}数据...")
    df = query_data(collection_name)
    
    if df.empty:
        print("未查询到数据")
        return df
    
    # 按时间戳排序
    df = df.sort_values('timestamp').reset_index(drop=True)
    
    if limit:
        df = df.tail(limit)
        print(f"限制数据条数: {limit}")
    
    print(f"加载完成，共 {len(df)} 条数据")
    return df


def prepare_correlation_data(df):
    """
    准备相关性分析数据
    
    Args:
        df: 原始数据DataFrame
    
    Returns:
        DataFrame: 包含当前delta_rate和下一个K线涨跌幅的数据
    """
    # 创建分析数据
    analysis_df = pd.DataFrame()
    analysis_df['timestamp'] = df['timestamp']
    analysis_df['delta_rate'] = df['delta_rate_amount']  
    analysis_df['candle_return'] = df['close'] / df['open'] - 1
    
    # 计算下一个K线的涨跌幅（百分比）
    analysis_df['next_candle_return'] = df['next_close'] / df['close'] - 1
    analysis_df['next_candle_return'] = analysis_df['next_candle_return'] * 100  # 转换为百分比
    
    # 移除包含NaN的行
    analysis_df = analysis_df.dropna()
    
    print(f"准备相关性分析数据完成，有效数据 {len(analysis_df)} 条")
    return analysis_df

def correlation_analysis(analysis_df):
    """
    进行相关性分析
    
    Args:
        analysis_df: 分析数据DataFrame
    
    Returns:
        dict: 相关性分析结果
    """
    print("\n=== 相关性分析 ===")
    
    # 提取数据
    delta_rate = analysis_df['delta_rate']
    next_return = analysis_df['candle_return']
    # next_return = analysis_df['next_candle_return']
    
    # 基本统计信息
    print(f"Delta Rate 统计:")
    print(f"  均值: {delta_rate.mean():.4f}")
    print(f"  标准差: {delta_rate.std():.4f}")
    print(f"  最小值: {delta_rate.min():.4f}")
    print(f"  最大值: {delta_rate.max():.4f}")
    
    print(f"\nNext Candle Return 统计:")
    print(f"  均值: {next_return.mean():.4f}%")
    print(f"  标准差: {next_return.std():.4f}%")
    print(f"  最小值: {next_return.min():.4f}%")
    print(f"  最大值: {next_return.max():.4f}%")
    
    # 皮尔逊相关系数
    pearson_corr, pearson_p = pearsonr(delta_rate, next_return)
    print(f"\n皮尔逊相关系数: {pearson_corr:.4f}")
    print(f"P值: {pearson_p:.4f}")
    print(f"显著性: {'显著' if pearson_p < 0.05 else '不显著'}")
    
    # 斯皮尔曼相关系数
    spearman_corr, spearman_p = spearmanr(delta_rate, next_return)
    print(f"\n斯皮尔曼相关系数: {spearman_corr:.4f}")
    print(f"P值: {spearman_p:.4f}")
    print(f"显著性: {'显著' if spearman_p < 0.05 else '不显著'}")
    
    # 分组分析
    print(f"\n=== 分组分析 ===")
    
    # 按delta_rate分组
    analysis_df['delta_group'] = pd.cut(analysis_df['delta_rate'], 
                                       bins=5, 
                                       labels=['很低', '低', '中', '高', '很高'])
    
    group_stats = analysis_df.groupby('delta_group')['next_candle_return'].agg([
        'count', 'mean', 'std', 'min', 'max'
    ]).round(4)
    
    print("Delta Rate分组统计:")
    print(group_stats)
    
    return {
        'pearson_corr': pearson_corr,
        'pearson_p': pearson_p,
        'spearman_corr': spearman_corr,
        'spearman_p': spearman_p,
        'group_stats': group_stats,
        'data_count': len(analysis_df)
    }

def create_visualizations(analysis_df, results):
    """
    创建可视化图表
    
    Args:
        analysis_df: 分析数据DataFrame
        results: 相关性分析结果
    """
    print("\n正在生成可视化图表...")
    
    # 创建图表
    fig, axes = plt.subplots(2, 2, figsize=(15, 12))
    fig.suptitle('Delta因子与价格相关性分析 (COAIUSDT - 1分钟)', fontsize=16, fontweight='bold')
    
    # 1. 散点图
    axes[0, 0].scatter(analysis_df['delta_rate'], analysis_df['next_candle_return'], 
                      alpha=0.6, s=20)
    axes[0, 0].set_xlabel('Delta Rate (next_close - close)')
    axes[0, 0].set_ylabel('下一K线涨跌幅 (%)')
    axes[0, 0].set_title(f'散点图 (相关系数: {results["pearson_corr"]:.4f})')
    axes[0, 0].grid(True, alpha=0.3)
    
    # 添加趋势线
    z = np.polyfit(analysis_df['delta_rate'], analysis_df['next_candle_return'], 1)
    p = np.poly1d(z)
    axes[0, 0].plot(analysis_df['delta_rate'], p(analysis_df['delta_rate']), 
                   "r--", alpha=0.8, linewidth=2)
    
    # 2. Delta Rate分布
    axes[0, 1].hist(analysis_df['delta_rate'], bins=50, alpha=0.7, color='skyblue')
    axes[0, 1].set_xlabel('Delta Rate')
    axes[0, 1].set_ylabel('频次')
    axes[0, 1].set_title('Delta Rate分布')
    axes[0, 1].grid(True, alpha=0.3)
    
    # 3. 下一K线涨跌幅分布
    axes[1, 0].hist(analysis_df['next_candle_return'], bins=50, alpha=0.7, color='lightcoral')
    axes[1, 0].set_xlabel('下一K线涨跌幅 (%)')
    axes[1, 0].set_ylabel('频次')
    axes[1, 0].set_title('下一K线涨跌幅分布')
    axes[1, 0].grid(True, alpha=0.3)
    
    # 4. 分组箱线图
    analysis_df['delta_group'] = pd.cut(analysis_df['delta_rate'], 
                                       bins=5, 
                                       labels=['很低', '低', '中', '高', '很高'])
    
    box_data = [analysis_df[analysis_df['delta_group'] == group]['next_candle_return'].values 
                for group in ['很低', '低', '中', '高', '很高']]
    
    axes[1, 1].boxplot(box_data, labels=['很低', '低', '中', '高', '很高'])
    axes[1, 1].set_xlabel('Delta Rate分组')
    axes[1, 1].set_ylabel('下一K线涨跌幅 (%)')
    axes[1, 1].set_title('分组箱线图')
    axes[1, 1].grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig('delta_factor_correlation_analysis.png', dpi=300, bbox_inches='tight')
    plt.show()
    
    print("图表已保存为: delta_factor_correlation_analysis.png")

def main():
    """主函数"""
    print("=== Delta因子与价格相关性分析 (HANAUSDT - 15分钟) ===\n")
    
    # 1. 加载数据
    # 专门分析HANAUSDT交易对 - 尝试15分钟周期
    df = load_data()  # 限制最近5000条数据
    
    if df.empty:
        print("没有数据可分析")
        return
    

    print(df)
    
    # 3. 准备相关性分析数据
    analysis_df = prepare_correlation_data(df)
    print('xxmxmxmxmxmmxmxm1')
    print(analysis_df)
    print(analysis_df[['timestamp', 'candle_return']])
    print('xxmxmxmxmxmmxmxm2')
    
    if analysis_df.empty:
        print("没有有效的分析数据")
        return
    
    # 4. 进行相关性分析
    results = correlation_analysis(analysis_df)
    
    # 5. 创建可视化图表
    create_visualizations(analysis_df, results)
    
    # 6. 输出结论
    print(f"\n=== 分析结论 ===")
    print(f"数据样本数: {results['data_count']}")
    print(f"皮尔逊相关系数: {results['pearson_corr']:.4f} ({'显著' if results['pearson_p'] < 0.05 else '不显著'})")
    print(f"斯皮尔曼相关系数: {results['spearman_corr']:.4f} ({'显著' if results['spearman_p'] < 0.05 else '不显著'})")
    
    if abs(results['pearson_corr']) > 0.1:
        direction = "正相关" if results['pearson_corr'] > 0 else "负相关"
        strength = "强" if abs(results['pearson_corr']) > 0.3 else "中等" if abs(results['pearson_corr']) > 0.1 else "弱"
        print(f"Delta因子与下一K线涨跌幅存在{strength}{direction}关系")
    else:
        print("Delta因子与下一K线涨跌幅相关性很弱")

if __name__ == "__main__":
    main()