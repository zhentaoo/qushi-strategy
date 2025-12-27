import pandas as pd

import api_core
import mongo_utils
import factor_utils


SYMBOL = "BTCUSDT"
INTERVAL = "1d"
LIMIT = 1000
COLLECTION_RAW = "btc_1d_kline"
COLLECTION_FACTOR = "btc_1d_kline_factor"

def fetch_btc_daily_klines(limit: int = LIMIT) -> pd.DataFrame | None:
    df = api_core.get_klines(SYMBOL, interval=INTERVAL, limit=limit)
    if df is None or df.empty:
        return None
    df = df.sort_values("timestamp").reset_index(drop=True)
    return df


def main() -> None:
    df = fetch_btc_daily_klines()
    if df is None or df.empty:
        print("未获取到 BTCUSDT 日线数据")
        return

    mongo_utils.delete_data(COLLECTION_RAW)
    mongo_utils.insert_data(COLLECTION_RAW, df)

    factor_df = factor_utils.compute_symbol_factor(df)
    mongo_utils.delete_data(COLLECTION_FACTOR)
    mongo_utils.insert_data(COLLECTION_FACTOR, factor_df)

    print(
        f"完成 BTCUSDT 日线数据抓取与因子计算，"
        f"原始 {len(df)} 条，因子 {len(factor_df)} 条"
    )


if __name__ == "__main__":
    main()

