import pandas as pd
import numpy as np
from testutils import vwap

# 创建20行简单的测试数据
data = {
    'high': [105.2, 106.5, 104.8, 107.3, 108.1, 107.5, 106.9, 109.2, 110.5, 109.8,
             111.2, 110.7, 112.3, 113.5, 112.8, 114.2, 115.0, 113.8, 116.2, 115.5],
    'low': [101.5, 102.3, 100.9, 103.2, 104.5, 103.8, 102.7, 105.1, 106.3, 105.5,
            107.1, 106.5, 108.2, 109.1, 108.5, 110.1, 111.2, 109.7, 112.3, 111.8],
    'close': [103.4, 104.2, 102.5, 105.1, 106.3, 105.2, 104.8, 107.3, 108.4, 107.2,
              109.3, 108.6, 110.5, 111.2, 110.6, 112.3, 113.1, 111.5, 114.2, 113.5],
    'volume': [520, 630, 480, 750, 820, 680, 590, 910, 1050, 880,
               1120, 950, 1230, 1350, 1180, 1420, 1500, 1280, 1620, 1450]
}

# 创建DataFrame
df = pd.DataFrame(data)

# 打印数据
print("测试数据:")
print(df)

# 计算VWAP
vwap_values = vwap(df)

# 打印VWAP结果
print("\nVWAP计算结果:")
print(vwap_values)

# 手动验证第一行数据
first_tp = (df['high'][0] + df['low'][0] + df['close'][0]) / 3.0
first_pv = first_tp * df['volume'][0]
first_vwap = first_pv / df['volume'][0]
print(f"\n手动验证第一行:")
print(f"典型价格 (TP) = ({df['high'][0]} + {df['low'][0]} + {df['close'][0]}) / 3 = {first_tp}")
print(f"价格 * 交易量 (PV) = {first_tp} * {df['volume'][0]} = {first_pv}")
print(f"VWAP = {first_pv} / {df['volume'][0]} = {first_vwap}")
print(f"函数计算结果: {vwap_values[0]}")