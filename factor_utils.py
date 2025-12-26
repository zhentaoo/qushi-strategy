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
 
    # ========= price shift =========
    for n in [1, 2, 3]:
        for col in ['open', 'close', 'high', 'low', 'volume']:
            g[f'{col}_pre{n}'] = g[col].shift(n)

    # ========= ROC，涨跌比 =========
    for n in [1, 4, 16, 32, 64, 96]:
        g[f'roc_{n}'] = (g['close'].pct_change(n) * 100)

    # ========= MA =========
    for n in [3, 5, 10, 15, 20, 30]:
        g[f'ma{n}'] = ta.sma(g['close'], length=n)
        g[f'ma{n}_pre1'] = g[f'ma{n}'].shift(1)
        g[f'ma{n}_pre2'] = g[f'ma{n}'].shift(2)
        g[f'ma{n}_pre3'] = g[f'ma{n}'].shift(3)

    # ========= volume MA & ratio =========
    for n in [3, 5, 10, 20, 60, 96]:
        g[f'volume_ma_{n}'] = g['volume'].rolling(n, min_periods=n).mean().shift(1)
        g[f'volume_ratio_{n}'] = g['volume'] / g[f'volume_ma_{n}']

    # ========= ATR =========
    g['atr'] = ta.atr(g['high'], g['low'], g['close'], length=14) # 生产用，因为实盘的currentdata就是前一个
    g['atr_pre1'] = g['atr'].shift(1) # 回测用（因为currentdata，要用前一个）

    # ========= ADX =========
    adx = ta.adx(g['high'], g['low'], g['close'], length=14)
    g['adx'] = safe_ta(adx, 'ADX_14')

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
