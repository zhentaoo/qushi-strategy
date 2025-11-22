"""
因子计算工具模块
包含K线数据处理和技术指标计算的通用函数
"""

import pandas as pd
import numpy as np
from datetime import datetime
from zoneinfo import ZoneInfo

def compute_symbol_factor(kline_data, symbol='', coin_info={}, is_runtime=False):
    # 简化版：假定存在 'symbol', 'interval', 'timestamp' 三列
    df = kline_data.copy() if isinstance(kline_data, pd.DataFrame) else pd.DataFrame(kline_data)

    # 统一数值类型
    for col in ['open', 'high', 'low', 'close', 'volume', 'taker_buy_volume']:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    # 全局排序（symbol, interval, timestamp）
    df = df.sort_values(['symbol', 'interval', 'timestamp']).reset_index(drop=True)

    def _calc_group(g):
        g = g.copy()
        # 移动平均线
        g['ma5'] = g['close'].rolling(5).mean()
        g['prev_ma5'] = g['ma5'].shift(1)
        
        g['ma10'] = g['close'].rolling(10).mean()
        g['prev_ma10'] = g['ma10'].shift(1)
        
        g['ma20'] = g['close'].rolling(20).mean()
        g['prev_ma20'] = g['ma20'].shift(1)
        
        g['ma30'] = g['close'].rolling(30).mean()
        g['ma60'] = g['close'].rolling(60).mean()
        g['ma96'] = g['close'].rolling(96).mean()

        # 24h：4:24
        g['roc_96'] = (g['close'] - g['close'].shift(96)) / g['close'].shift(96) * 100
        # 16h 周期：4 * 16 = 64
        g['roc_64'] = (g['close'] - g['close'].shift(64)) / g['close'].shift(64) * 100
        # 8h 周期：4 * 8
        g['roc_32'] = (g['close'] - g['close'].shift(32)) / g['close'].shift(32) * 100
        # 4h 周期: 4 * 4
        g['roc_16'] = (g['close'] - g['close'].shift(16)) / g['close'].shift(16) * 100
        # 1h 周期
        g['roc_4'] = (g['close'] - g['close'].shift(4)) / g['close'].shift(4) * 100

        # 上一轮涨跌
        g['prev_open'] = g['open'].shift(1)
        g['prev_close'] = g['close'].shift(1)
        g['prev2_open'] = g['open'].shift(2)
        g['prev2_close'] = g['close'].shift(2)

        g['price_change_pct'] = g['close'].pct_change() * 100
        g['roc_1'] = (g['close'] - g['close'].shift(1)) / g['close'].shift(1) * 100
        
        # 成交量与量比
        g['volume_ma5'] = g['volume'].rolling(5).mean()
        g['volume_ma10'] = g['volume'].rolling(10).mean()
        g['volume_ma60'] = g['volume'].rolling(60).mean()
        g['volume_ma96'] = g['volume'].rolling(96).mean()
        
        g['volume_ratio_5'] = g['volume'] / g['volume_ma5']
        g['volume_ratio_10'] = g['volume'] / g['volume_ma10']
        g['volume_ratio_60'] = g['volume'] / g['volume_ma60']
        g['volume_ratio_96'] = g['volume'] / g['volume_ma96']

        # delta amount比: 主动买卖比例，ratio越高，做空动能越强
        g['delta_amount_ratio'] = g['taker_buy_amount'] / g['taker_sell_amount']

        # 上影线因子：上影线长度及其相对比例（对高低区间归一化）
        g['upper_shadow'] = np.where(
            g['close'] >= g['open'],
            g['high'] - g['close'],
            g['high'] - g['open']
        )
        g['upper_shadow_ratio'] = np.where(
            (g['high'] - g['low']) > 0,
            g['upper_shadow'] / (g['high'] - g['low']),
            np.nan
        )

        # ATR波动指标
        g['H-L']  = g['high'] - g['low']
        g['H-PC'] = (g['high'] - g['close'].shift(1)).abs()
        g['L-PC'] = (g['low'] - g['close'].shift(1)).abs()
        g['tr'] = g[['H-L', 'H-PC', 'L-PC']].max(axis=1)
        g['atr'] = g['tr'].rolling(14).mean()

        g['atr_prev'] = g['atr'].shift(1)
        g['atr_change_pct'] = g['tr'] / g['atr_prev']

        # 最终的分数指标
        g['score'] = np.abs(g['roc_1']) + g['volume_ratio_10'] + 2 * g['delta_amount_ratio']
        g['prev1_score'] = g['score'].shift(1)
        g['prev2_score'] = g['score'].shift(2)
        return g

    # 分组计算（symbol+interval）
    df = df.groupby(['symbol', 'interval'], group_keys=False).apply(_calc_group)
    
    return df
