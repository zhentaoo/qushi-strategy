import pandas as pd
import numpy as np
import pandas_ta_classic as ta

def safe_ta(result, col=None):
    if result is None:
        return np.nan
    if col is None:
        return result
    return result[col]

def compute_single_symbol_factor(g: pd.DataFrame):
    """
    g: 单 symbol + 单 interval + 已按时间排序
    """
    if len(g) < 65:
        return g

    g = g.sort_values('timestamp').copy()
    
    g['taker_sell_amount'] = g['amount'] - g['taker_buy_amount']

    # ========= delta ratio（安全处理） =========
    eps = 1e-8
    # 修复：确保分母非零，使用 np.maximum 避免除以零或非常小的数
    g['delta_buy_ratio'] = g['taker_buy_amount'] / np.maximum(g['taker_sell_amount'], eps)
    g['delta_sell_ratio'] = g['taker_sell_amount'] / np.maximum(g['taker_buy_amount'], eps)


    # ========= price shift =========
    for n in [1, 2, 3]:
        for col in ['open', 'close', 'high', 'low', 'volume']:
            g[f'{col}_pre{n}'] = g[col].shift(n)

    # ========= ROC，涨跌比 =========
    for n in [1, 4, 16, 32, 64, 96]:
        g[f'roc_{n}'] = (g['close'].pct_change(n) * 100)

    # ========= Donchian Channel =========
    g['high_64'] = g['high'].rolling(window=64, min_periods=64).max().shift(1)
    g['low_64'] = g['low'].rolling(window=64, min_periods=64).min().shift(1)

    # ========= MA =========
    for n in [3, 5, 10, 15, 20, 30]:
        # g[f'ma{n}'] = ta.sma(g['close'], length=n)
        # g[f'ma{n}_pre1'] = g[f'ma{n}'].shift(1)
        # g[f'ma{n}_pre2'] = g[f'ma{n}'].shift(2)
        # g[f'ma{n}_pre3'] = g[f'ma{n}'].shift(3)

        g[f'ma{n}'] = ta.sma(g['close'], length=n).shift(1)
        g[f'ma{n}_pre1'] = g[f'ma{n}'].shift(2)
        g[f'ma{n}_pre2'] = g[f'ma{n}'].shift(3)
        g[f'ma{n}_pre3'] = g[f'ma{n}'].shift(4)

    # ========= volume MA & ratio =========
    for n in [3, 5, 10, 20, 60, 96]:
        g[f'volume_ma_{n}'] = g['volume'].rolling(n, min_periods=n).mean().shift(1)
        g[f'volume_ratio_{n}'] = g['volume'] / g[f'volume_ma_{n}']


    # ========= Bollinger =========
    bb = ta.bbands(g['close'], length=20, std=2)
    g['bollinger_mid'] = safe_ta(bb, 'BBM_20_2.0').shift(1)
    g['bollinger_upper'] = safe_ta(bb, 'BBU_20_2.0').shift(1)
    g['bollinger_lower'] = safe_ta(bb, 'BBL_20_2.0').shift(1)

    # ========= ATR =========
    g['atr'] = ta.atr(g['high'], g['low'], g['close'], length=14).shift(1)
    g['natr'] = g['atr'] / g['close'] * 100

    # ========= ADX =========
    adx = ta.adx(g['high'], g['low'], g['close'], length=14)
    g['adx'] = safe_ta(adx, 'ADX_14').shift(1)  


    return g


def compute_symbol_factor(df: pd.DataFrame):
    """
    df: 单一 interval 的整张表，包含多个 symbol
    """
    df = df.sort_values(['symbol', 'timestamp'])

    result = []

    for symbol, g in df.groupby('symbol'):
        g = compute_single_symbol_factor(g)
        result.append(g)

    return pd.concat(result, ignore_index=True)
