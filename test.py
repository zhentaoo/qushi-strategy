#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
import time
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import api_core
import mongo_utils

# # 交易参数配置
# KLINE_INTERVAL = "15m"  # K线周期（15分钟）

# # 读取MongoDB最新时间点的数据并生成做空信号

# try:
#     df = mongo_utils.query_recent_data_by_symbol('runtime_symbol_factor_15min_kline', limit_per_symbol=1)
#     print(df)
# except Exception as e:
#     print(f"读取因子集合失败: {e}")

# if df is None or df.empty:
#     print("因子集合无数据")


# latest_ts = int(df['timestamp'].max())
# latest_df = df[df['timestamp'] == latest_ts].copy()

# # 时间戳校验：若最新数据与当前时间相差≥3分钟，则认为数据过期，终止逻辑
# latest_ts_close = int(latest_df['close_time'].max())
# try:
#     now_ms = int(time.time() * 1000)
#     diff_ms = abs(now_ms - latest_ts_close)
#     if diff_ms >= 3 * 60 * 1000:
#         latest_str = datetime.fromtimestamp(latest_ts_close/1000, tz=ZoneInfo('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M:%S')
#         now_str = datetime.fromtimestamp(now_ms/1000, tz=ZoneInfo('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M:%S')
#         api_core.send_custom_wechat_message(f"数据过期：最新close timestamp={latest_ts_close}({latest_str})，当前时间={now_str}，相差{diff_ms/1000:.0f}s，停止执行")
# except Exception as e:
#     print(f"时间戳校验失败: {e}")
    

print(int(time.time() * 1000))
