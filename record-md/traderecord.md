归零：如果不加均线直接归零
if (
    row['boll_pct_b_high'] > 1.15 and  # 重要参数，一旦放低将会有很多问题
    row['intraday_change_pct'] > 2 and
    row['volume'] < row['vol_ma2'] and
    # row['vol_ma2'] > (row['vol_ma10'] * 1.5) and # 这个参数一旦调高，影响收入
    row['long_upper_shadow'] > 0.15
    # row['close'] < row['high_250'] and
    # row['close_ma20'] < row['close_ma250']
):

# 7倍
if (
    row['boll_pct_b_high'] > 1.15 and
    # row['boll_pct_b_high_yesterday'] > 1.15 and  # 重要参数，一旦放低将会有很多问题
    # row['boll_pct_b_high'] <  row['boll_pct_b_high_yesterday'] and  # 重要参数，一旦放低将会有很多问题
    row['volume'] < row['vol_ma2'] and
    row['intraday_change_pct'] > 2 and
    row['long_upper_shadow'] > 0.2 and
    row['close_ma20'] < row['close_ma250']

    # row['vol_ma2'] > (row['vol_ma10'] * 1.5) and # 这个参数一旦调高，影响收入
    # row['close'] < row['high_200'] and
    # row['close_ma30'] < row['close_ma200']

    # row['close'] < row['high_250'] and
):

7倍+
if (
    row['boll_pct_b_high'] > 1.15 and  # 重要参数，一旦放低将会有很多问题
    row['intraday_change_pct'] > 2 and
    row['volume'] < row['vol_ma2'] and
    row['long_upper_shadow'] > 0.15 and
    row['close_ma20'] < row['close_ma250']
):


6倍
if (
    row['boll_pct_b_high'] > 1.15 and  # 重要参数，一旦放低将会有很多问题
    row['intraday_change_pct'] > 2 and
    row['volume'] < row['vol_ma2'] and
    row['long_upper_shadow'] > 0.15 and
    row['close'] < row['high_250'] and
    row['close_ma20'] < row['close_ma250']
):


5倍：
if (
    row['boll_pct_b_high'] > 1 and
    row['intraday_change_pct'] > 3 and
    row['volume'] < row['vol_ma2'] and
    row['vol_ma3'] > (row['vol_ma20'] * 2) and
    row['long_upper_shadow'] > 0.15 and
    row['close'] < row['high_250'] and
    row['close_ma20'] < row['close_ma250']
):