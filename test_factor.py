import pandas as pd
import pandas_ta_classic as ta
import numpy as np
import mongo_utils

data = mongo_utils.query_recent_data_by_symbol('symbol_15min_kline', limit_per_symbol=200)

data = data[data['symbol'] == 'ALLUSDT'].sort_values('timestamp').reset_index(drop=True)


adx_result = data.ta.adx(
    high=data['high'],
    low=data['low'],
    close=data['close'],
    length=14,
    append=True
)

print("ADX Calculated Columns:", adx_result.columns.tolist())
print("All Data Columns:", data.columns.tolist())

# mongo_utils.insert_data('test_adx', data)

print(data)


