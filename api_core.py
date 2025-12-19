#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
import pandas as pd
import requests
import hmac
import hashlib
import time
import math
from datetime import datetime, timedelta, timezone
from urllib.parse import urlencode

from zoneinfo import ZoneInfo

BINANCE_API_KEY = "F2PUCpjCPcO5CK9ApY9GqMafTHEiimPOyV9HNCX2dB6vpPeBeR3VEQ6H0n2Dpu94"
BINANCE_SECRET_KEY = "uJs5eLZGCbyiA0fTHAbYZkgCLK0pH1SQ6NlFZyMHAorg5dB1jAXeWxsxIs7Oy14o"
WECHAT_WEBHOOK_URL = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=be0e8741-7bf5-4222-a0d1-df88ac7748fb"
BASE_URL = "https://fapi.binance.com"


def signed_request(method, path, params=None):
    """币安API签名请求"""
    params = params or {}
    params["timestamp"] = int(time.time() * 1000)
    qs = "&".join([f"{k}={v}" for k,v in params.items()])
    sig = hmac.new(BINANCE_SECRET_KEY.encode(), qs.encode(), hashlib.sha256).hexdigest()
    qs += "&signature=" + sig
    headers = {"X-MBX-APIKEY": BINANCE_API_KEY}
    return requests.request(method, BASE_URL+path, params=qs, headers=headers).json()

# 获取账户信息和持仓接口
def get_account_position():
    """获取账户持仓"""
    try:
        positions = signed_request("GET", "/fapi/v3/positionRisk")
        active_positions = [p for p in positions if float(p.get("positionAmt", 0)) != 0]
        return active_positions
    except Exception as e:
        print(f"获取账户信息失败: {str(e)}")
        return None, None

# 获取账户余额接口
def get_balance():
    """获取账户余额（只返回USDT余额）"""
    try:
        balances = signed_request("GET", "/fapi/v3/balance")
        usdt_balance = next((b for b in balances if b.get("asset") == "USDT"), None)
        return usdt_balance
    except Exception as e:
        print(f"获取余额失败: {str(e)}")
        return None

# 获取交易规则和交易对
def get_exchange_info():
    """获取交易所信息，筛选出有效的USDT交易对"""
    try:
        url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
        ex_resp = requests.get(url)
        ex_resp.raise_for_status()
        exchange_info = ex_resp.json()
        return exchange_info
    except Exception as e:
        print(f"获取交易所信息失败: {e}")
        return None

# 获取24小时行情数据接口，龙虎榜单接口
def get_ticker_24hr():
    """获取24小时行情数据"""
    try:
        url = "https://fapi.binance.com/fapi/v1/ticker/24hr"
        response = requests.get(url)
        response.raise_for_status()
        ticker_data = response.json()
        return ticker_data
    except Exception as e:
        print(f"获取24小时行情数据失败: {e}")
        return None

# 获取K线数据接口
def get_klines(symbol, interval="5m", limit=100, startTime=None, endTime=None, filter_full_time=True):
    try:
        url = "https://fapi.binance.com/fapi/v1/klines"
        params = {'symbol': symbol, 'interval': interval, 'limit': limit}
        
        # 只有在提供了startTime参数时才添加到请求参数中
        if startTime is not None:
            params['startTime'] = startTime
            
        # 只有在提供了endTime参数时才添加到请求参数中
        if endTime is not None:
            params['endTime'] = endTime
            
        response = requests.get(url, params=params)
        response.raise_for_status()
        raw_kline_data = response.json()
        
        # 若最后一根K线未完成（收盘时间在未来），则去掉最后一根
        current_ms = int(time.time() * 1000)
        if raw_kline_data and int(raw_kline_data[-1][6]) > current_ms and filter_full_time:
            raw_kline_data = raw_kline_data[:-1]
        
        # 将列表格式转换为字典格式，并计算衍生字段
        kline_data = []
        for kline in raw_kline_data:
            # 基础字段
            timestamp = int(kline[0])
            open_price = float(kline[1])
            high_price = float(kline[2])
            low_price = float(kline[3])
            close_price = float(kline[4])
            volume = float(kline[5])
            close_time = int(kline[6])
            amount = float(kline[7])
            count = int(kline[8])
            taker_buy_volume = float(kline[9])
            taker_buy_amount = float(kline[10])
            
            # 计算衍生字段
            taker_sell_amount = amount - taker_buy_amount
            delta_rate_amount = (taker_buy_amount - taker_sell_amount) / amount if amount != 0 else 0
            candle_return = (close_price - open_price) / open_price * 100 if open_price != 0 else 0
            
            kline_dict = {
                'symbol': symbol,
                'interval': interval,
                'date_str': datetime.fromtimestamp(timestamp / 1000, tz=ZoneInfo('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M:%S'),
                'close_time_str': datetime.fromtimestamp(close_time / 1000, tz=ZoneInfo('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M:%S'),
                'timestamp': timestamp,               # 开盘时间
                'open': open_price,                   # 开盘价
                'high': high_price,                   # 最高价
                'low': low_price,                     # 最低价
                'close': close_price,                 # 收盘价
                'volume': volume,                     # 成交量
                'close_time': close_time,             # 收盘时间
                'amount': amount,                     # 成交额
                'count': count,                       # 成交笔数
                'taker_buy_volume': taker_buy_volume, # 主动买入成交量
                'taker_buy_amount': taker_buy_amount, # 主动买入成交额
                'taker_sell_amount': taker_sell_amount, # 主动卖出成交额
                'delta_rate_amount': delta_rate_amount, # 买卖盘差异率
                'candle_return': candle_return,       # 蜡烛收益率
                'ignore': kline[11]                   # 忽略字段
            }
            kline_data.append(kline_dict)
        
        return pd.DataFrame(kline_data)
    except Exception as e:
        print(f"获取K线数据失败: {e}")
        return None

# 获取单个币种价格接口
def get_price(symbol):
    """获取单个币种的当前价格"""
    try:
        price_response = requests.get(BASE_URL + "/fapi/v1/ticker/price", params={"symbol": symbol})
        price_response.raise_for_status()
        return float(price_response.json()["price"])
    except Exception as e:
        print(f"获取价格失败: {e}")
        return None

# 获取最优挂单价格接口
def get_book_ticker(symbol):
    """获取最优买卖挂单价格
    
    Args:
        symbol (str): 交易对符号，如 'DOGEUSDT'
    
    Returns:
        dict: 最优挂单信息
            - bidPrice (float): 最优买价
            - bidQty (float): 最优买量
            - askPrice (float): 最优卖价
            - askQty (float): 最优卖量
        None: 获取失败时返回None
    """
    try:
        url = BASE_URL + "/fapi/v1/ticker/bookTicker"
        params = {"symbol": symbol}
        response = requests.get(url, params=params)
        response.raise_for_status()
        
        data = response.json()
        return {
            'bidPrice': float(data['bidPrice']),  # 最优买价
            'bidQty': float(data['bidQty']),      # 最优买量
            'askPrice': float(data['askPrice']),  # 最优卖价
            'askQty': float(data['askQty'])       # 最优卖量
        }
    except Exception as e:
        print(f"获取最优挂单价格失败: {e}")
        return None

# 计算下单数量接口
def get_quantity(symbol, usdt_amount):
    """计算合适的下单数量，同时返回价格精度"""
    try:
        # 获取交易对信息
        exchange_info = get_exchange_info()
        rule = next(s for s in exchange_info["symbols"] if s["symbol"] == symbol)
        print('====rule====')
        print(rule)
        
        step = float(next(f["stepSize"] for f in rule["filters"] if f["filterType"] == "LOT_SIZE"))
        min_qty = float(next(f["minQty"] for f in rule["filters"] if f["filterType"] == "LOT_SIZE"))
        min_notional = float(next(f["notional"] for f in rule["filters"] if f["filterType"] == "MIN_NOTIONAL"))
        price_precision = rule.get("pricePrecision", 4)
        
        # 获取当前价格
        price = get_price(symbol)
        
        raw = usdt_amount / price
        qty = math.floor(raw / step) * step
        if qty < min_qty:
            qty = min_qty
        if qty * price < min_notional:
            qty = math.ceil(min_notional / price / step) * step

        quantity_precision = rule.get("quantityPrecision", 0)
        qty = round(qty, quantity_precision)
        return qty, price_precision
    except Exception as e:
        print(f"计算下单数量失败: {e}")
        return None, 4

# 下单接口(市价)
def place_order(signal,side,usdt_amount = 6, leverage = 1):
    """执行交易订单"""
    print("=== 执行交易订单 ===")
    if not signal:
        print("没有交易信号，跳过下单")
        return None

    symbol = signal['symbol']
    set_leverage(symbol, leverage)

    usdt_balance = get_balance()
    print(f'当前USDT余额: {usdt_balance}')
    
    try:
        qty, price_precision = get_quantity(symbol, usdt_amount)
        if qty is None:
            return None
        
        price = float(signal['price'])
        print(f"准备{'做空' if side == 'SELL' else '做多'} {symbol}: 数量={qty}, 价格={price}")
        order_result = signed_request("POST", "/fapi/v1/order", {
            "symbol": symbol,
            "side": side,
            "type": "MARKET",
            "quantity": qty
        })
        print(f"订单结果: {order_result}")

        if 'orderId' in order_result:
            return {
                'success': True,
                'symbol': symbol,
                'side': side,
                'quantity': qty,
                'order_result': order_result,
                'signal': signal
            }
        else:
            return {
                'success': False,
                'symbol': symbol,
                'side': side,
                'error': order_result,
                'signal': signal
            }
    except Exception as e:
        error_msg = f"{'卖出' if side == 'SELL' else '买入'}下单失败: {e}"
        print(error_msg)
        return {
            'success': False,
            'symbol': symbol,
            'error': error_msg,
            'signal': signal
        }

# 止盈止损委托接口
# side: 原始持仓方向
# stop_loss_price_ratio: 止损位价格相对于当前价格的比例
def place_tp_sl_order(symbol, originSide, quantity, take_profit_price=None, stop_loss_price=None):
    print("=== 下止盈止损委托单 ===")
    
    # 平仓方向
    reverseSide = None

    if originSide == "SELL":
        reverseSide = "BUY"
    else:
        reverseSide = "SELL"

    results = []
    
    try:
        # 获取交易对精度信息
        exchange_info = get_exchange_info()
        if not exchange_info:
            return {'success': False, 'error': '获取交易对信息失败'}
        
        rule = next(s for s in exchange_info["symbols"] if s["symbol"] == symbol)
        price_precision = rule.get("pricePrecision", 4)
        
        # 下止盈单
        if take_profit_price:
            tp_price = round(float(take_profit_price), price_precision)
            
            tp_order = signed_request("POST", "/fapi/v1/order", {
                "symbol": symbol,
                "side": reverseSide,
                "type": "TAKE_PROFIT_MARKET",
                "quantity": quantity,
                "stopPrice": tp_price,
                "reduceOnly": "true",
                "timeInForce": "GTC",
                "workingType": "CONTRACT_PRICE"
            })
            
            results.append({
                'type': 'take_profit',
                'order': tp_order,
                'price': tp_price
            })
            print(f"止盈单结果: {tp_order}")
        
        # 下止损单
        if stop_loss_price:
            sl_price = round(float(stop_loss_price), price_precision)
            
            sl_order = signed_request("POST", "/fapi/v1/order", {
                "symbol": symbol,
                "side": reverseSide,
                "type": "STOP_MARKET",
                "quantity": quantity,
                "stopPrice": sl_price,
                "reduceOnly": "true",
                "timeInForce": "GTC",
                "workingType": "MARK_PRICE"
            })
            
            results.append({
                'type': 'stop_loss',
                'order': sl_order,
                'price': sl_price
            })
            print(f"止损单结果: {sl_order}")
        
        return {
            'success': True,
            'symbol': symbol,
            'orders': results
        }
        
    except Exception as e:
        error_msg = f"下止盈止损委托失败: {str(e)}"
        print(error_msg)
        return {
            'success': False,
            'error': error_msg,
            'symbol': symbol
        }

# 清仓订单接口（市价）
def close_position(symbol, position_amt):
    """清仓订单"""
    try:
        close_result = signed_request("POST", "/fapi/v1/order", {
            "symbol": symbol,
            "side": "BUY" if position_amt < 0 else "SELL",  # 空头用买入平仓，多头用卖出平仓
            "type": "MARKET",
            "quantity": abs(position_amt),
            "reduceOnly": "true"
        })
        return close_result
    except Exception as e:
        print(f"清仓失败: {str(e)}")
        return None

# 设置杠杆倍数接口
def set_leverage(symbol, leverage=1):
    """设置杠杆倍数"""
    try:
        leverage_result = signed_request("POST", "/fapi/v1/leverage", {
            "symbol": symbol,
            "leverage": leverage
        })
        return leverage_result
    except Exception as e:
        print(f"设置杠杆倍数失败: {str(e)}")
        return None

# 微信通知接口
def send_wechat_message(signal=None, order_result=None):
    """发送微信通知，根据参数自动判断消息类型和内容
    
    Args:
        signal: 交易信号数据
        order_result: 订单结果数据
    """
    print("=== 发送微信通知 ===")
    
    # 如果没有交易信号，直接跳过，不发送通知
    if signal is None:
        print("未找到符合条件的交易信号，跳过发送通知")
        return None
    
    try:
        # 根据参数自动判断消息类型和生成消息内容
        if order_result and order_result.get('success'):
            # 交易成功
            message = f"✅ 交易执行成功\n\n币种: {signal['symbol']}\n方向: 做空 (SELL)\n数量: {order_result['quantity']}\n价格: {signal['price']}\n订单ID: {order_result.get('order_result', {}).get('orderId', 'N/A')}"
        elif order_result and not order_result.get('success'):
            # 交易失败
            error = order_result.get('error', '未知错误')
            message = f"❌ 交易执行失败\n\n币种: {signal['symbol']}\n24h涨幅: {signal.get('priceChangePercent', 0)}%\n错误信息: {error}"
        else:
            # 有信号但未下单
            message = f"📊 发现交易信号但未执行\n\n币种: {signal['symbol']}\n24h涨幅: {signal.get('priceChangePercent', 0)}%\n买卖比率: {signal.get('delta_rate_amount', 0):.4f}"
        
        # 添加时间戳
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        message += f"\n\n⏰ 时间: {current_time}"
        
        data = {"msgtype": "text", "text": {"content": message}}
        response = requests.post(WECHAT_WEBHOOK_URL, json=data)
        
        if response.status_code == 200:
            print("微信通知发送成功")
            return True
        else:
            print(f"微信通知发送失败: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"发送微信通知失败: {e}")
        # 发送错误通知
        try:
            error_message = f"❌ 微信通知发送失败\n错误信息: {str(e)}\n时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            data = {"msgtype": "text", "text": {"content": error_message}}
            requests.post(WECHAT_WEBHOOK_URL, json=data)
        except:
            pass
        return False


def send_custom_wechat_message(message):
    """发送自定义微信通知"""
    try:
        data = {"msgtype": "text", "text": {"content": message}}
        response = requests.post(WECHAT_WEBHOOK_URL, json=data)
        
        if response.status_code == 200:
            print("自定义微信通知发送成功")
            return True
        else:
            print(f"自定义微信通知发送失败: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"发送自定义微信通知失败: {e}")
        return False

# 撤销全部订单接口（指定交易对）
def cancel_all_orders(symbol):
    print(f"=== 撤销 {symbol} 的所有挂单 ===")
    
    try:
        # 调用币安API撤销指定交易对的所有挂单
        result = signed_request("DELETE", "/fapi/v1/allOpenOrders", {"symbol": symbol})
        print(f"撤销所有挂单结果: {result}")
        return {'success': True, 'symbol': symbol, 'result': result}
    except Exception as e:
        error_msg = f"撤销所有挂单失败: {str(e)}"
        print(error_msg)
        return {
            'success': False,
            'symbol': symbol,
            'error': error_msg
        }

# 获取未完成挂单列表（可选按symbol过滤）
def get_open_orders(symbol: str | None = None):
    try:
        params = {}
        if symbol:
            params["symbol"] = symbol
        orders = signed_request("GET", "/fapi/v1/openOrders", params)
        return orders
    except Exception as e:
        print(f"获取未完成挂单失败: {e}")
        return []

# 撤销账户当前全部挂单（遍历交易对逐个撤销）
def cancel_all_open_orders():
    try:
        open_orders = get_open_orders()
        if not isinstance(open_orders, list) or len(open_orders) == 0:
            print("目前无未完成挂单")
            return {"success": True, "message": "无未完成挂单", "symbols": []}
        symbols = {o.get("symbol") for o in open_orders if o.get("symbol")}
        print(f"准备撤销 {len(open_orders)} 条挂单，涉及 {len(symbols)} 个交易对")
        errors = []
        for sym in symbols:
            res = cancel_all_orders(sym)
            if not res.get('success', False):
                errors.append({"symbol": sym, "error": res.get('error')})
        return {"success": len(errors) == 0, "symbols": list(symbols), "errors": errors}
    except Exception as e:
        print(f"撤销全部挂单失败: {e}")
        return {"success": False, "error": str(e)}
