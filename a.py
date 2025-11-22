import pandas as pd
import mongo_utils

df = mongo_utils.query_recent_data_by_symbol('runtime_symbol_factor_15min_kline', limit_per_symbol=200)
df = df[df['symbol'] == 'COTIUSDT']
print(df)

latest_df = df[df['timestamp'] == 1762725600000].copy()

latest_df.to_csv('latest_df.csv', index=False)

if latest_df.empty:
    print("没有找到最新时间点的数据")
    

latest_df.to_csv('latest_df.csv', index=False)
filtered_df = latest_df[
    (latest_df['open'] < latest_df['close'])
    & (latest_df['score'] > 15)
    & (latest_df['score'] < 27)
].copy()

print('****')
print(filtered_df)
print('****')

# print(top_df)