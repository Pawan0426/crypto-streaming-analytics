import os
import glob
import pandas as pd
from datetime import datetime, date

# Directories
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SILVER_DIR = os.path.join(BASE_DIR, "data", "silver")
GOLD_DIR = os.path.join(BASE_DIR, "data", "gold")

# Crypto symbols to process
symbols = ["BTC-USD", "ETH-USD", "ADA-USD", "DOGE-USD"]

def aggregate_symbol_hourly(symbol, processing_date):
    input_dir = os.path.join(SILVER_DIR, processing_date, symbol)
    output_dir = os.path.join(GOLD_DIR, processing_date, symbol)
    os.makedirs(output_dir, exist_ok=True)

    parquet_files = glob.glob(f"{input_dir}/*.parquet")
    if not parquet_files:
        print(f"⚠️ No data for {symbol} on {processing_date}")
        return

    # Read and combine all files
    dfs = []
    for file in parquet_files:
        df = pd.read_parquet(file)
        df["source_file"] = os.path.basename(file)  # optional
        dfs.append(df)
    df = pd.concat(dfs, ignore_index=True)

    # Ensure timestamp is datetime
    df["fetched_at"] = pd.to_datetime(df["fetched_at"], utc=True)
    df["time"] = pd.to_datetime(df["time"], utc=True)
    df.set_index("fetched_at", inplace=True)
    df.sort_index(inplace=True)

    # Base hourly aggregation
    hourly = df.resample("1h").agg({
        "price": ["min", "max", "mean", "first", "last"],
        "volume": "sum",
        "symbol": "first",
        "time": ["min", "max"],
        "source_file": "first"
    }).dropna()

    # Flatten MultiIndex columns
    hourly.columns = [
        "_".join(col) if isinstance(col, tuple) else col
        for col in hourly.columns
    ]
    hourly.reset_index(inplace=True)

    # Rename columns
    hourly.rename(columns={
        "fetched_at": "hour",
        "price_min": "min_price",
        "price_max": "max_price",
        "price_mean": "avg_price",
        "price_first": "first_price",
        "price_last": "last_price",
        "volume_sum": "total_volume",
        "time_min": "first_seen",
        "time_max": "last_seen",
        "symbol_first": "symbol",
        "source_file_first": "source_file"
    }, inplace=True)

    # Add derived metrics
    hourly["price_change_pct"] = (
        (hourly["last_price"] - hourly["first_price"]) / hourly["first_price"]
    ) * 100
    hourly["date"] = processing_date
    hourly["hour_of_day"] = hourly["hour"].dt.hour

    # Save output
    timestamp = int(datetime.now().timestamp())
    output_file = os.path.join(output_dir, f"hourly_summary_{timestamp}.parquet")
    hourly.to_parquet(output_file, index=False)
    print(f"✅ Hourly gold data for {symbol} saved to {output_file}")

def run_hourly_aggregation():
    processing_date = date.today().isoformat()
    for symbol in symbols:
        aggregate_symbol_hourly(symbol, processing_date)

if __name__ == "__main__":
    run_hourly_aggregation()
