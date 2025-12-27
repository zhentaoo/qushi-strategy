from requests import api
import pandas as pd
import api_core
import mongo_utils
import factor_utils




mongo_utils.remove_duplicates('symbol_1h_kline')

# a = api_core.get_account_position()
# print(a)

# api_core.close_position(a[0]['symbol'],a[0]['positionAmt'])

# api_core.cancel_all_stop_orders(a[0]['symbol'])

# a = api_core.place_stop_market_order(a[0]['symbol'], 0.16)

# 获取K线数据
# klines = api_core.get_klines("BTCUSDT", interval="15m", limit=90)

# # print("=== K线数据（前3条）===")
# print(klines)
# mongo_utils.insert_data('btc_15m_kline', klines)

# # 计算因子
# df_klines = factor_utils.compute_symbol_factor(klines)
# mongo_utils.insert_data('btc_15m_kline_factor', df_klines)

# print("\n=== DataFrame信息 ===")
# print(f"DataFrame形状: {df_klines.shape}")
# print(f"列名: {list(df_klines.columns)}")

# print("\n=== DataFrame前5行 ===")
# print(df_klines.head())

# print("\n=== 第一行数据 ===")
# print(df_klines.iloc[0])  # 使用iloc[0]访问第一行
# print(df_klines[df_klines.columns[0]])  # 访问第一行的timestamp列

