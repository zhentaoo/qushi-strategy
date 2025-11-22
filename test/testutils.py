def vwap(df):
    tp = (df['high'] + df['low'] + df['close']) / 3.0
    pv = tp * df['volume']
    cum_pv = pv.cumsum()
    cum_vol = df['volume'].cumsum()
    return cum_pv / (cum_vol + 1e-12)