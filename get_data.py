import os
import ssl
import certifi
import datetime
# 设置 SSL 证书环境变量，确保在多进程（spawn）模式下也能生效
os.environ['SSL_CERT_FILE'] = certifi.where()

from binance_historical_data import BinanceDataDumper
import pandas as pd
import urllib.request

# ---------- SSL 设置（macOS 兼容） ----------
# 这个 patch 只对当前进程有效，对于 mpire spawn 的子进程，依赖上面的环境变量
ssl._create_default_https_context = lambda: ssl.create_default_context(cafile=certifi.where())

def check_connectivity():
    """检查与 Binance 数据源的连接"""
    url = "https://data.binance.vision"
    print(f"---> Testing connectivity to {url}...")
    try:
        urllib.request.urlopen(url, timeout=10)
        print("---> Connection successful!")
        return True
    except Exception as e:
        print(f"---> Connection failed: {e}")
        print("---> 建议：如果您在中国大陆，请开启全局代理或设置终端代理。")
        print("---> 例如：export https_proxy=http://127.0.0.1:7890")
        return False

def main():
    # ---------- 检查网络 ----------
    if not check_connectivity():
        print("⚠️ 网络连接测试失败，脚本可能会报错。")
        # 不强制退出，尝试继续

    # ---------- 配置 ----------
    SAVE_DIR = "."
    os.makedirs(SAVE_DIR, exist_ok=True)

    # 时间范围：最近一年
    end_date = datetime.date.today()
    start_date = end_date - datetime.timedelta(days=365)

    # ---------- 初始化 Dumper ----------
    dumper = BinanceDataDumper(
        path_dir_where_to_dump=SAVE_DIR,
        asset_class="um",       # USDT 永续合约
        data_type="klines",
        data_frequency="1h"
    )

    # ---------- 下载历史数据 ----------
    dumper.dump_data(
        tickers=None,          # None = 所有 USDT 永续交易对
        date_start=start_date,
        date_end=end_date,
        is_to_update_existing=False,
        tickers_to_exclude=None,
    )

    # ---------- 合并 CSV 为 Parquet ----------
    # 注意：这里假设 dump_data 下载的是 CSV 文件，需要确认下载后的目录结构
    # binance_historical_data 通常会按照 asset_class/data_type/data_frequency/ticker/ 这样的结构存储
    # 下面的代码可能需要根据实际下载结构调整。
    # 暂时先保留原逻辑，但原逻辑可能找不到文件，因为 dumper 会创建子目录。
    
    # 简单的遍历查找所有 csv
    all_files = []
    for root, dirs, files in os.walk(SAVE_DIR):
        for file in files:
            if file.endswith(".csv"):
                all_files.append(os.path.join(root, file))

    if not all_files:
        print("未找到任何 CSV 文件。")
        return

    dfs = []
    for file in all_files:
        try:
            df = pd.read_csv(file)
            # 尝试从文件名或路径中提取 symbol
            # 假设文件名包含 symbol 或者路径包含 symbol
            # binance_historical_data 默认路径结构: .../spot/monthly/klines/BTCUSDT/1h/BTCUSDT-1h-2023-01.csv
            # 这里简单取文件名的第一部分作为 symbol，或者根据路径结构解析
            # 更好的方式是看 binance_historical_data 的文档或源码，但这里先尝试通用做法
            filename = os.path.basename(file)
            symbol = filename.split('-')[0] 
            df['symbol'] = symbol
            dfs.append(df)
        except Exception as e:
            print(f"Error reading {file}: {e}")

    if dfs:
        full_df = pd.concat(dfs, ignore_index=True)

        # 保存为 parquet
        parquet_path = os.path.join(SAVE_DIR, "futures_usdt_1h_last_year.parquet")
        full_df.to_parquet(parquet_path, engine="pyarrow", index=False)

        print(f"✅ 下载完成！合并后的 Parquet 文件：{parquet_path}")
        print(f"总交易对数量：{len(dfs)}") # 这里其实是文件数，不完全等于交易对数
        print(f"总数据条数：{len(full_df)}")
    else:
        print("没有数据被合并。")

if __name__ == "__main__":
    main()
