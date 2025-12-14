import pandas as pd
import pandas_ta_classic as ta
import numpy as np
import mongo_utils
import warnings

# 忽略 pandas 的 FutureWarnings，特别是关于 chained assignment 的
warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.simplefilter(action='ignore', category=pd.errors.SettingWithCopyWarning)

def main():
    # 1. 获取数据
    print("正在获取数据...")
    try:
        data = mongo_utils.query_recent_data_by_symbol('symbol_15min_kline', limit_per_symbol=200)
        # 过滤出 BTCUSDT 或者其他有数据的币种，防止数据混合
        if not data.empty and 'symbol' in data.columns:
            # 取第一个symbol的数据
            symbol = data['symbol'].iloc[0]
            data = data[data['symbol'] == symbol].copy()
            # 确保按时间排序
            if 'timestamp' in data.columns:
                data = data.sort_values('timestamp')
            print(f"使用 {symbol} 的数据进行计算, 数据量: {len(data)}")
        elif data.empty:
            print("警告: 未查询到数据")
    except Exception as e:
        print(f"获取数据失败: {e}")
        data = pd.DataFrame()

    if data.empty:
        print("使用模拟数据...")
        data = pd.DataFrame({
            'open': np.random.uniform(100, 200, 100),
            'high': np.random.uniform(200, 210, 100),
            'low': np.random.uniform(90, 100, 100),
            'close': np.random.uniform(100, 200, 100),
            'volume': np.random.uniform(1000, 5000, 100),
        })
        # pandas_ta 某些指标可能需要 datetime index
        data.index = pd.date_range(start='2024-01-01', periods=100, freq='15min')

    # 2. 列出所有支持的指标 (List available indicators)
    print("\n=== 所有支持的指标列表 ===")
    try:
        # 尝试通过 DataFrame accessor 获取指标列表
        # pandas_ta_classic 通常继承自 pandas_ta，应该支持 data.ta.indicators()
        indicators_list = data.ta.indicators(as_list=True)
        if indicators_list:
            print(f"共发现 {len(indicators_list)} 个指标:")
            # 分组打印，每行5个
            for i in range(0, len(indicators_list), 5):
                print(", ".join(indicators_list[i:i+5]))
        else:
            print("未能获取指标列表 (data.ta.indicators 返回空)")
    except AttributeError:
        print("data.ta.indicators() 方法不存在")
    except Exception as e:
        print(f"列出指标时出错: {e}")

    # 3. 计算所有指标 (Calculate all indicators)
    print("\n=== 正在计算所有指标 (Strategy: All) ===")
    try:
        # 使用 'All' 策略计算所有指标
        # 注意: 这可能会产生大量的列，并且计算时间较长
        # timed=True 会显示计算耗时
        # verbose=True 显示进度
        # multiprocessed=True 开启多进程（默认），但需要在 if __name__ == '__main__': 下运行
        data.ta.strategy(name='All', verbose=True, timed=True, multiprocessed=True)
        
        print("\n=== 计算完成 ===")
        print(f"结果包含 {len(data.columns)} 列")
        print("前20个列名:")
        print(data.columns.tolist()[:20])
        if len(data.columns) > 20:
            print("...")
        
        # 打印最后一行数据的非空值数量
        last_row = data.iloc[-1]
        print(f"\n最后一行非空值数量: {last_row.count()} / {len(last_row)}")

    except Exception as e:
        print(f"计算指标时出错: {e}")

if __name__ == "__main__":
    try:
        # 某些系统可能需要显式调用 freeze_support
        from multiprocessing import freeze_support
        freeze_support()
    except ImportError:
        pass
    
    main()
