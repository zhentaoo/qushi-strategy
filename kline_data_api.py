#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
K线数据API接口
从MongoDB读取K线数据并提供HTTP接口给前端调用
"""

from flask import Flask, jsonify, request, send_from_directory, send_file
from flask_cors import CORS
import pandas as pd
from datetime import datetime, timedelta
import json
import os
import sys
import mongo_utils

app = Flask(__name__)
CORS(app)  # 允许跨域请求

def format_kline_data(df):
    """格式化K线数据为ECharts需要的格式（不计算均线）"""
    if df.empty:
        return {
            'dates': [],
            'klineData': [],
            'volumes': [],
            'buyAmounts': [],
            'sellAmounts': [],
            'deltaRates': []
        }
    
    # 按时间排序
    if 'open_time' in df.columns:
        df = df.sort_values('open_time').reset_index(drop=True)
    elif 'timestamp' in df.columns:
        df = df.sort_values('timestamp').reset_index(drop=True)
    else:
        df = df.reset_index(drop=True)
    
    # 格式化时间，优先使用 date_str
    dates = []
    for _, row in df.iterrows():
        if 'date_str' in row and isinstance(row['date_str'], str) and row['date_str']:
            dates.append(row['date_str'])
        elif 'open_time' in row and row['open_time']:
            try:
                dt = pd.to_datetime(int(row['open_time']), unit='ms')
                dates.append(dt.strftime('%Y-%m-%d %H:%M:%S'))
            except Exception:
                dates.append('')
        else:
            dates.append('')
    
    # K线数据 [open, close, low, high]
    kline_data = []
    close_prices = []
    for _, row in df.iterrows():
        open_price = float(row.get('open', 0))
        close_price = float(row.get('close', 0))
        low_price = float(row.get('low', 0))
        high_price = float(row.get('high', 0))
        kline_data.append([open_price, close_price, low_price, high_price])
        close_prices.append(close_price)
    
    # 成交额/买卖/Delta
    volumes = []
    buy_amounts = []
    sell_amounts = []
    delta_rates = []
    candle_returns = []
    for i, row in df.iterrows():
        total_amount = float(row.get('amount', 0))
        taker_buy_amount = float(row.get('taker_buy_amount', 0))
        taker_sell_amount = total_amount - taker_buy_amount
        delta_rate = float(row.get('delta_rate_amount', 0)) if row.get('delta_rate_amount') not in (None, '') else 0
        candle_return = float(row.get('candle_return', 0)) if row.get('candle_return') not in (None, '') else 0
        direction = 1 if i == 0 or close_prices[i] >= close_prices[i-1] else -1
        volumes.append([i, direction, total_amount])
        buy_amounts.append(taker_buy_amount)
        sell_amounts.append(-taker_sell_amount)
        delta_rates.append(delta_rate)
        candle_returns.append(candle_return)
    
    return {
        'dates': dates,
        'klineData': kline_data,
        'volumes': volumes,
        'buyAmounts': buy_amounts,
        'sellAmounts': sell_amounts,
        'deltaRates': delta_rates,
        'candleReturns': candle_returns
    }

@app.route('/api/kline', methods=['GET'])
def get_kline_data():
    """获取K线数据API（从 coin_history 按 symbol 与 interval 查询）"""
    try:
        symbol = request.args.get('symbol', 'BTCUSDT')
        interval = request.args.get('interval', '5m')
        limit = int(request.args.get('limit', 1000))
        
        # 从MongoDB coin_history集合获取数据（按时间倒序取最近 limit 条）
        db = mongo_utils.get_db()
        collection = db['coin_history']
        cursor = collection.find({'symbol': symbol, 'interval': interval}).sort('open_time', -1).limit(limit)
        records = list(cursor)
        
        if not records:
            return jsonify({
                'success': False,
                'message': f'没有找到 {symbol} {interval} 的数据',
                'data': None
            })
        
        # 转为DataFrame并按时间升序
        df = pd.DataFrame(records)
        if 'open_time' in df.columns:
            df = df.sort_values('open_time').reset_index(drop=True)
        
        formatted_data = format_kline_data(df)
        
        return jsonify({
            'success': True,
            'message': f'成功获取 {symbol} {interval} 的 {len(formatted_data["dates"]) } 条K线数据',
            'data': formatted_data,
            'symbol': symbol,
            'interval': interval,
            'count': len(formatted_data["dates"]) 
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'获取数据失败: {str(e)}',
            'data': None
        })

@app.route('/api/symbols', methods=['GET'])
def get_available_symbols():
    """获取 coin_history 中可用的交易对列表"""
    try:
        db = mongo_utils.get_db()
        collection = db['coin_history']
        symbols = sorted(collection.distinct('symbol'))
        return jsonify({
            'success': True,
            'data': symbols
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'获取交易对列表失败: {str(e)}',
            'data': None
        })

@app.route('/api/latest', methods=['GET'])
def get_latest_data():
    """获取最新的K线数据（来自 coin_history）"""
    try:
        symbol = request.args.get('symbol', 'BTCUSDT')
        interval = request.args.get('interval', '1m')
        
        db = mongo_utils.get_db()
        collection = db['coin_history']
        cursor = collection.find({'symbol': symbol, 'interval': interval}).sort('open_time', -1).limit(1)
        records = list(cursor)
        if not records:
            return jsonify({
                'success': False,
                'message': '没有数据',
                'data': None
            })
        latest = records[0]
        # 格式化时间
        if 'date_str' in latest and isinstance(latest['date_str'], str):
            latest['timestamp'] = latest['date_str']
        elif 'open_time' in latest:
            latest['timestamp'] = pd.to_datetime(int(latest['open_time']), unit='ms').strftime('%Y-%m-%d %H:%M:%S')
        return jsonify({
            'success': True,
            'data': latest
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'获取最新数据失败: {str(e)}',
            'data': None
        })

@app.route('/kline_chart.html')
def serve_chart():
    """提供K线图表HTML文件"""
    try:
        return send_from_directory('.', 'kline_chart.html')
    except Exception as e:
        return f"文件未找到: {str(e)}", 404

@app.route('/')
def index():
    return send_file('kline_chart.html')

if __name__ == '__main__':
    port = int(os.getenv('PORT', '5002'))
    print("启动K线数据API服务...")
    print("访问地址:")
    print(f"- 主页: http://localhost:{port}")
    print(f"- K线图表: http://localhost:{port}/kline_chart.html")
    print(f"- API接口: http://localhost:{port}/api/kline")
    print(f"- 可用数据: http://localhost:{port}/api/symbols")
    app.run(host='0.0.0.0', port=port, debug=True)