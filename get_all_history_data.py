#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
import pandas as pd
import zipfile
import io
import time
import os
from datetime import datetime
from dateutil.relativedelta import relativedelta
from zoneinfo import ZoneInfo
import api_core
import mongo_utils
from concurrent.futures import ThreadPoolExecutor

def get_month_range(start_ts):
    """
    Generate (year, month) tuples from start_ts to PREVIOUS month.
    Binance monthly data does not include the current incomplete month.
    If start_ts is older than 2 years, start from 2 years ago.
    """
    start_date = datetime.fromtimestamp(start_ts / 1000, tz=ZoneInfo('UTC'))
    current_date = datetime.now(tz=ZoneInfo('UTC'))
    
    # Calculate previous month (last complete month)
    # If today is May 2025, we want up to April 2025.
    # replace day=1 ensures we are comparing months correctly
    last_complete_month = current_date.replace(day=1, hour=0, minute=0, second=0, microsecond=0) - relativedelta(months=1)
    
    # 2 years ago from now
    two_years_ago = current_date - relativedelta(years=2)
    
    # Use the later of the two dates (optimization: max 2 years data)
    effective_start_date = max(start_date, two_years_ago)
    
    # Start from the first day of the effective start month
    current = effective_start_date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    
    # Iterate until last_complete_month (inclusive)
    while current <= last_complete_month:
        yield current.year, current.month
        current += relativedelta(months=1)

def process_csv_data(df, symbol):
    """
    Process raw CSV dataframe to match schema.
    """
    # 1. Check and remove header if present
    if not df.empty:
        first_cell = str(df.iloc[0, 0])
        if 'open_time' in first_cell:
            df = df.iloc[1:].copy()
    
    # 2. Assign raw CSV column names
    # As per user instruction, the CSV columns are:
    # open_time,open,high,low,close,volume,close_time,quote_volume,count,taker_buy_volume,taker_buy_quote_volume,ignore
    df.columns = [
        'open_time', 'open', 'high', 'low', 'close', 'volume', 
        'close_time', 'quote_volume', 'count', 
        'taker_buy_volume', 'taker_buy_quote_volume', 'ignore'
    ]

    # 3. Rename to match system schema (mongo_utils and api_core expectations)
    # open_time -> timestamp
    # quote_volume -> amount
    # taker_buy_quote_volume -> taker_buy_amount
    df.rename(columns={
        'open_time': 'timestamp',
        'quote_volume': 'amount',
        'taker_buy_quote_volume': 'taker_buy_amount'
    }, inplace=True)

    # 4. Convert types to numeric
    numeric_cols = [
        'timestamp', 'open', 'high', 'low', 'close', 'volume', 
        'close_time', 'amount', 'count', 'taker_buy_volume', 'taker_buy_amount'
    ]
    
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # 5. Add metadata
    df['symbol'] = symbol
    df['interval'] = '1h'
    
    # 6. Drop 'ignore' column
    if 'ignore' in df.columns:
        df.drop(columns=['ignore'], inplace=True)
        
    return df

def process_task(task):
    """
    Worker function to process a single download task.
    """
    symbol = task['symbol']
    year = task['year']
    month = task['month']
    url = task['url']
    save_dir = task['save_dir']
    zip_filename = task['zip_filename']
    csv_filename = zip_filename.replace('.zip', '.csv')
    month_str = f"{month:02d}"

    try:
        # Check if CSV already exists
        local_csv_path = os.path.join(save_dir, csv_filename)
        if os.path.exists(local_csv_path):
            # Already exists, skipping
            print(f"  Skipping {zip_filename} (already exists)")
            return

        print(f"  Downloading {zip_filename}...")
        try:
            # Add timeout to avoid hanging
            response = requests.get(url, timeout=30)
        except requests.exceptions.RequestException as e:
            print(f"  Failed to download {url}: {e}")
            return

        if response.status_code == 404:
            # File not found (common for very recent or very old months)
            # print(f"  File not found (404): {url}")
            return
        
        if response.status_code != 200:
            print(f"  Failed to download {url}, status: {response.status_code}")
            return

        # Unzip and read CSV
        try:
            with zipfile.ZipFile(io.BytesIO(response.content)) as z:
                # Find CSV in zip
                file_list = z.namelist()
                target_file = None
                for f in file_list:
                    if f.endswith('.csv'):
                        target_file = f
                        break
                
                if not target_file:
                    print(f"  No CSV found in zip for {zip_filename}")
                    return
                    
                # Read content from zip
                content = z.read(target_file)
                
                # Save CSV to local disk
                with open(local_csv_path, 'wb') as local_f:
                    local_f.write(content)
                print(f"  Saved CSV to {local_csv_path}")
                
                # Read into DataFrame for DB insertion
                try:
                    df = pd.read_csv(io.BytesIO(content), header=None)
                        
                    if df.empty:
                        print(f"  Empty CSV for {zip_filename}")
                        return
                        
                    # Process data
                    processed_df = process_csv_data(df, symbol)
                    
                    # Insert into MongoDB
                    collection_name = 'symbol_1h_kline'
                    
                    try:
                        mongo_utils.insert_data(collection_name, processed_df)
                    except Exception as e:
                        if "E11000" in str(e): 
                            print(f"  Data already exists in DB for {symbol} {year}-{month_str}")
                        else:
                            print(f"  Error inserting data for {symbol}: {e}")
                except Exception as e:
                     print(f"  Error processing CSV content for {symbol}: {e}")

        except zipfile.BadZipFile:
            print(f"  Bad Zip File: {url}")

    except Exception as e:
        print(f"  Error processing task {url}: {e}")

def download_and_save_data():
    # 1. Get exchange info
    print("Getting exchange info...")
    exchange_info = api_core.get_exchange_info()
    if not exchange_info:
        print("Failed to get exchange info")
        return

    symbols = exchange_info.get('symbols', [])
    
    usdt_pairs = [s for s in symbols if s.get('status') == 'TRADING' and s.get('quoteAsset') == 'USDT']
    
    
    base_url = "https://data.binance.vision/data/futures/um/monthly/klines"
    
    # Directory to save CSV files
    save_dir = 'data/csv'
    os.makedirs(save_dir, exist_ok=True)
    
    # 2. Generate tasks and download.txt
    print("Generating download list...")
    all_tasks = []
    
    for idx, symbol_info in enumerate(usdt_pairs):
        symbol = symbol_info['symbol']
        onboard_date = symbol_info.get('onboardDate')
        
        if not onboard_date:
            print(f"Skipping {symbol}: No onboard date")
            continue
            
        # Check if onboard date is older than 3 months
        onboard_ts = onboard_date / 1000 # Convert to seconds
        onboard_dt = datetime.fromtimestamp(onboard_ts, tz=ZoneInfo('UTC'))
        current_dt = datetime.now(tz=ZoneInfo('UTC'))
        
        # 3 months ago from now
        three_months_ago = current_dt - relativedelta(months=3)
        
        if onboard_dt > three_months_ago:
            print(f"Skipping {symbol}: New listing (< 3 months), onboarded {onboard_dt.date()}")
            continue

        # Iterate months
        for year, month in get_month_range(onboard_date):
            month_str = f"{month:02d}"
            filename = f"{symbol}-1h-{year}-{month_str}.zip"
            url = f"{base_url}/{symbol}/1h/{filename}"
            
            all_tasks.append({
                'symbol': symbol,
                'year': year,
                'month': month,
                'url': url,
                'zip_filename': filename,
                'save_dir': save_dir
            })
            
    # Write download.txt
    download_txt_path = 'download.txt'
    try:
        with open(download_txt_path, 'w') as f:
            for task in all_tasks:
                f.write(task['url'] + '\n')
        print(f"Generated {len(all_tasks)} tasks in {download_txt_path}")
    except Exception as e:
        print(f"Error writing download.txt: {e}")
    
    # 3. Filter missing files
    tasks_to_run = []
    for task in all_tasks:
        csv_filename = task['zip_filename'].replace('.zip', '.csv')
        local_path = os.path.join(save_dir, csv_filename)
        
        # Check if file exists
        if not os.path.exists(local_path):
            tasks_to_run.append(task)
            
    print(f"Found {len(tasks_to_run)} files missing. Starting download...")
    
    if not tasks_to_run:
        print("All files are already downloaded.")
        return
    
    # 4. Multi-threaded processing
    # Use 5 threads to be safe with rate limits
    max_workers = 5
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        futures = [executor.submit(process_task, task) for task in tasks_to_run]
        
        # Wait for completion (implicitly done by context manager, but we might want progress)
        # We can use as_completed to show progress if needed, but let's keep it simple.
        pass
        
    print("All tasks completed.")

if __name__ == "__main__":
    download_and_save_data()
