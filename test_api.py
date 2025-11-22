import pandas as pd
import api_core
import mongo_utils
import factor_utils

# 获取K线数据
klines = api_core.get_klines("BTCUSDT", interval="15m", limit=90)

# print("=== K线数据（前3条）===")
print(klines)
mongo_utils.insert_data('btc_15m_kline', klines)

# 计算因子
df_klines = factor_utils.compute_symbol_factor(klines)
mongo_utils.insert_data('btc_15m_kline_factor', df_klines)

# print("\n=== DataFrame信息 ===")
# print(f"DataFrame形状: {df_klines.shape}")
# print(f"列名: {list(df_klines.columns)}")

# print("\n=== DataFrame前5行 ===")
# print(df_klines.head())

# print("\n=== 第一行数据 ===")
# print(df_klines.iloc[0])  # 使用iloc[0]访问第一行
# print(df_klines[df_klines.columns[0]])  # 访问第一行的timestamp列

