# transformation/aggregate_trade_gold.py
import os
import pandas as pd
from datetime import datetime, date
import glob
from transformation.validation import validate_trade_data

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SILVER_DIR = os.path.join(BASE_DIR, "data", "silver", "trades")
GOLD_DIR = os.path.join(BASE_DIR, "data", "gold", "trades")

symbols = ["BTC-USD", "ETH-USD", "ADA-USD", "DOGE-USD"]
time_buckets = ["30min", "1h"]

def aggregate_trade_data(symbol, processing_date, hour):
    input_file = os.path.join(SILVER_DIR, processing_date, symbol, f"trades_hour_{hour}.parquet")
    if not os.path.exists(input_file):
        print(f"⚠️ Missing silver file for {symbol} hour {hour}")
        return

    df = pd.read_parquet(input_file)

    issues = validate_trade_data(df, symbol)
    if issues:
        for issue in issues:
            print("⚠️", issue)
        return

    df["fetched_at"] = pd.to_datetime(df["fetched_at"], utc=True)
    df.set_index("fetched_at", inplace=True)

    for bucket in time_buckets:
        aggregated = df.resample(bucket).agg(
            price_min=('price', 'min'),
            price_max=('price', 'max'),
            price_avg=('price', 'mean'),
            total_volume=('size', 'sum'),
            avg_trade_size=('size', 'mean'),
            trade_count=('trade_id', 'count'),
            buy_volume=('side', lambda x: (x == 'buy').sum()),
            sell_volume=('side', lambda x: (x == 'sell').sum())
        ).dropna()

        if aggregated.empty:
            continue

        aggregated.reset_index(inplace=True)
        aggregated["symbol"] = symbol
        aggregated["date"] = processing_date
        aggregated["bucket"] = bucket

        output_dir = os.path.join(GOLD_DIR, processing_date, symbol)
        os.makedirs(output_dir, exist_ok=True)
        output_file = os.path.join(
            output_dir, f"trade_summary_{bucket}_hour_{hour}.parquet"
        )
        aggregated.to_parquet(output_file, index=False)
        print(f"✅ {bucket} aggregated data saved for {symbol} → {output_file}")

def run_gold_trade_aggregation():
    processing_date = date.today().isoformat()
    current_hour = datetime.utcnow().hour
    for symbol in symbols:
        aggregate_trade_data(symbol, processing_date, current_hour)

if __name__ == "__main__":
    run_gold_trade_aggregation()
