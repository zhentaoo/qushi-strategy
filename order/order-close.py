import time, hmac, hashlib, requests
import json

API_KEY = "rrpH9QZYWI69dP72JXRJ2iQMy0f0zu0p5LNtfSw5u6F2iQVl9Gmm9K2hfQQE7Hvt"
API_SECRET = "ceR2Tw39oljdHbljIgP3QdUB9rTeOgYBQiOUUgzKNyA5jd4lckDZInJSN7VpklRE"
BASE = "https://fapi.binance.com"

def signed_request(method, path, params=None):
    params = params or {}
    params["timestamp"] = int(time.time() * 1000)
    qs = "&".join([f"{k}={v}" for k,v in params.items()])
    sig = hmac.new(API_SECRET.encode(), qs.encode(), hashlib.sha256).hexdigest()
    qs += "&signature=" + sig
    headers = {"X-MBX-APIKEY": API_KEY}
    return requests.request(method, BASE+path, params=qs, headers=headers).json()

def clear_position(active_positions):
    try:
        if not active_positions:
            print("当前没有持仓，无需平仓")
            return {"success": True, "message": "当前没有持仓，无需平仓", "closed_positions": []}
        
        # 3. 对每个持仓进行平仓操作
        closed_positions = []
        for position in active_positions:
            symbol = position["symbol"]
            position_amt = float(position["positionAmt"])
            
            print(f"正在平仓: {symbol}, 数量: {abs(position_amt)}")
            
            # 使用closePosition参数直接平仓
            order_result = signed_request("POST", "/fapi/v1/order", {
                "symbol": symbol,
                "side": "SELL" if position_amt > 0 else "BUY",
                "type": "MARKET",
                "quantity": abs(position_amt),
                "reduceOnly": "true"
            })
            
            closed_positions.append({
                "symbol": symbol,
                "result": order_result
            })
            
            print(f"平仓结果: {order_result}")
        
        return {
            "success": True,
            "message": f"成功平仓 {len(closed_positions)} 个持仓",
            "closed_positions": closed_positions
        }
        
    except Exception as e:
        error_msg = f"平仓过程中发生错误: {str(e)}"
        print(error_msg)
        return {"success": False, "message": error_msg}

def get_account_info():
    try:
        # 获取账户信息
        account_info = signed_request("GET", "/fapi/v2/account")
        
        # 获取持仓信息 (使用v3版本API)
        positions = signed_request("GET", "/fapi/v3/positionRisk")
        active_positions = [p for p in positions if float(p.get("positionAmt", 0)) != 0]
        
        # 打印账户余额
        if "availableBalance" in account_info:
            print(f"可用余额: {account_info['availableBalance']} USDT")
        
        # 打印当前持仓
        if active_positions:
            print("\n当前持仓:")
            for pos in active_positions:
                print(pos)
                symbol = pos["symbol"]
                amount = float(pos["positionAmt"])
                entry_price = float(pos["entryPrice"])
                mark_price = float(pos["markPrice"])
                pnl = float(pos["unRealizedProfit"])
                
                direction = "多" if amount > 0 else "空"
                print(f"{symbol}: {direction} {abs(amount)} 张, 入场价: {entry_price}, 标记价: {mark_price}, 未实现盈亏: {pnl} USDT")
        else:
            print("\n当前没有持仓")
            
        return account_info, active_positions
    
    except Exception as e:
        print(f"获取账户信息失败: {str(e)}")
        return None, None

if __name__ == "__main__":
    # 显示账户信息
    print("\n=== 账户信息 ===")
    account_info, active_positions = get_account_info()


    result = clear_position(active_positions)
    
    