
import pandas as pd
import numpy as np

try:
    df = pd.DataFrame({'volume': [300, 100, 600], 'volume_ma_10': [100, 100, 100]})
    # Expected: 
    # Row 0: 200 < 300 < 500 -> True
    # Row 1: 200 < 100 < 500 -> False
    # Row 2: 200 < 600 < 500 -> False
    
    result = (df['volume_ma_10'] * 2 < df['volume'] < df['volume_ma_10'] * 5)
    print("Result:", result)
except Exception as e:
    print("Error:", e)
