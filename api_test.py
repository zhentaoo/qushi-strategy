from pandas import api
import api_core
import mongo_utils
import factor_utils

# api_test = api_core.get_rolling_window_ticker_mini(['BTCUSDT','COAIUSDT'], '15m')
# print(api_test)

# active_positions = api_core.get_account_position()
# print(active_positions)


# balance = api_core.get_balance()
# print(balance)

# get_quantity = api_core.get_quantity('STOUSDT', 10)
# print(get_quantity)


# set_leverage = api_core.set_leverage('BNBUSDT', 1)
# print(set_leverage)


# get_account_position = api_core.get_account_position()
# print(get_account_position)


get_exchange_info = api_core.get_exchange_info()
print(get_exchange_info)


# klines_data = api_core.get_klines('COAIUSDT', '1m', 300)
# print(klines_data   )

# processed_data = factor_utils.compute_symbol_factor(klines_data, 'COAIUSDT')
# print(processed_data)

# processed_data['next_close'] = processed_data['close'].shift(-1)
# print("原始K线数据获取成功")
# print(f"数据处理完成，共处理 {len(processed_data)} 条记录")
# # print(f"示例数据: {processed_data[0] if processed_data else None}")

# # 写入MongoDB
# mongo_utils.insert_data('COAIUSDT_1m_processed', processed_data)
# print("数据已写入MongoDB")


# api_ticker_24hr = api_core.get_ticker_24hr()
# print(api_ticker_24hr)

# api_core.place_limit_order('DOGEUSDT', 'BUY', 6)
# api_core.place_limit_order('DOGEUSDT', 'BUY', 6)

# api_core.close_position_limit('DOGEUSDT', 18)


# api_core.place_limit_order('DOGEUSDT', 'SELL', 6,0.35, 0.9)



# open_orders = api_core.get_open_orders()
# print(open_orders)


# api_core.cancel_all_open_orders()
# api

# get_price = api_core.get_price('COAIUSDT')

# print(get_price, type(get_price))