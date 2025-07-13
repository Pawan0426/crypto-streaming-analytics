# transformation/transform_trades_silver.py
import os
import glob
import json
import pandas as pd
from datetime import datetime, date, timedelta, timezone
from transformation.validation import validate_trade_data


BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
RAW_TRADES_DIR = os.path.join(BASE_DIR, "data", "raw", "trades")
SILVER_DIR = os.path.join(BASE_DIR, "data", "silver", "trades")

symbols = ["BTC-USD", "ETH-USD", "ADA-USD", "DOGE-USD"]

def process_trade_file(filepath):
    with open(filepath, "r") as f:
        data = json.load(f)
    return pd.DataFrame(data)

def transform_trades(symbol, processing_date, hour):
    input_dir = os.path.join(RAW_TRADES_DIR, symbol, processing_date)
    output_dir = os.path.join(SILVER_DIR, processing_date, symbol)
    os.makedirs(output_dir, exist_ok=True)

    trade_files = glob.glob(os.path.join(input_dir, "trades_*.json"))
    if not trade_files:
        print(f"⚠️ No trade files for {symbol} on {processing_date}")
        return

    dfs = []
    for file in trade_files:
        try:
            df = process_trade_file(file)
            dfs.append(df)
        except Exception as e:
            print(f"⚠️ Failed to process {file}: {e}")

    if not dfs:
        print(f"⚠️ No valid trade data for {symbol}")
        return

    df = pd.concat(dfs, ignore_index=True)
    df["price"] = df["price"].astype(float)
    df["size"] = df["size"].astype(float)
    df["time"] = pd.to_datetime(df["time"], utc=True)
    df["fetched_at"] = pd.to_datetime(df["fetched_at"], utc=True)

    # Filter to only the current hour
    #start_time = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
    #end_time = start_time + timedelta(hours=1)
    start_time = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    end_time = start_time + timedelta(hours=1)
    df = df[(df["fetched_at"] >= start_time) & (df["fetched_at"] < end_time)]

    if df.empty:
        print(f"⚠️ No trade data for {symbol} in hour {hour}")
        return

    df = df.sort_values("time")

    issues = validate_trade_data(df, symbol, level="silver")
    if issues:
        print(f"❌ Validation failed for {symbol}:")
        for issue in issues:
            print(f"   - {issue}")
        return

    output_file = os.path.join(output_dir, f"trades_hour_{hour}.parquet")
    df.to_parquet(output_file, index=False)
    print(f"✅ Saved silver trade data for {symbol} hour {hour} → {output_file}")

def run_trade_transformation():
    processing_date = date.today().isoformat()
    #current_hour = datetime.utcnow().hour
    current_hour = datetime.now(timezone.utc).hour
    for symbol in symbols:
        transform_trades(symbol, processing_date, current_hour)

if __name__ == "__main__":
    run_trade_transformation()
