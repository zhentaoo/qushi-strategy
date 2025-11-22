import time, hmac, hashlib, requests, math


# https://www.binance.com/bapi/futures/v1/private/future/order/place-order
# newClientOrderId: "web_coin_bs99ukk7rw4vixj7cy7104n"
# placeType: "order-form"
# positionSide: "BOTH"
# quantity: 0.001
# reduceOnly: false
# side: "SELL"
# symbol: "BTCUSDT"
# type: "MARKET"


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

def get_quantity(symbol, usdt_amount):
    info = requests.get(BASE+"/fapi/v1/exchangeInfo").json()
    rule = next(s for s in info["symbols"] if s["symbol"] == symbol)
    step = float(next(f["stepSize"] for f in rule["filters"] if f["filterType"]=="LOT_SIZE"))
    min_qty = float(next(f["minQty"] for f in rule["filters"] if f["filterType"]=="LOT_SIZE"))
    min_notional = float(next(f["notional"] for f in rule["filters"] if f["filterType"]=="MIN_NOTIONAL"))
    price = float(requests.get(BASE+"/fapi/v1/ticker/price", params={"symbol":symbol}).json()["price"])

    raw = usdt_amount / price
    qty = math.floor(raw/step)*step
    if qty < min_qty: qty = min_qty
    if qty*price < min_notional: qty = math.ceil(min_notional/price/step)*step
    return int(qty)

def place_order(symbol, side, usdt_amount):
    qty = get_quantity(symbol, usdt_amount)
    print(f"Placing {side} order for {qty} {symbol} at market price")
    return signed_request("POST","/fapi/v1/order",{
    # return signed_request("POST","/fapi/v1/order/test",{
        "symbol":symbol,
        "side":side,
        "type":"MARKET",
        "quantity":qty
    })

# 示例：500 USDT 买入 ADAUSDT
# print(place_order("ALPINEUSDT","SELL",10))
print(place_order("SQDUSDT","SELL",10))

