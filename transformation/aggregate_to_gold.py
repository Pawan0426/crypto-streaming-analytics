import os
import pandas as pd
from datetime import datetime, date
import glob

# Define base directories
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SILVER_DIR = os.path.join(BASE_DIR, "data", "silver")
GOLD_DIR = os.path.join(BASE_DIR, "data", "gold")

# Symbols to process
symbols = ["BTC-USD", "ETH-USD", "ADA-USD", "DOGE-USD"]


def compute_hourly_aggregates(df, symbol, source_file):
    df["time"] = pd.to_datetime(df["time"])
    df["hour"] = df["time"].dt.floor("h")

    result = []

    for hour, group in df.groupby("hour"):
        group_sorted = group.sort_values("time")

        first_price = group_sorted["price"].iloc[0]
        last_price = group_sorted["price"].iloc[-1]

        result.append({
            "symbol": symbol,
            "hour": hour,
            "min_price": group["price"].min(),
            "max_price": group["price"].max(),
            "avg_price": group["price"].mean(),
            "total_volume": group["volume"].sum(),
            "first_seen": group_sorted["time"].iloc[0],
            "last_seen": group_sorted["time"].iloc[-1],
            "first_price": first_price,
            "last_price": last_price,
            "price_change_pct": ((last_price - first_price) / first_price) * 100 if first_price != 0 else None,
            "source_file": os.path.basename(source_file),
            "date": hour.date(),
            "hour_of_day": hour.hour
        })

    return pd.DataFrame(result)


def aggregate_symbol_hourly(symbol, processing_date):
    input_dir = os.path.join(SILVER_DIR, processing_date, symbol)
    output_dir = os.path.join(GOLD_DIR, processing_date, symbol)
    os.makedirs(output_dir, exist_ok=True)

    parquet_files = glob.glob(f"{input_dir}/*.parquet")
    if not parquet_files:
        print(f"⚠️ No silver data found for {symbol} on {processing_date}")
        return

    all_results = []

    for pq_file in parquet_files:
        df = pd.read_parquet(pq_file)
        hourly_df = compute_hourly_aggregates(df, symbol, pq_file)
        all_results.append(hourly_df)

    if all_results:
        final_df = pd.concat(all_results, ignore_index=True)
        timestamp = int(datetime.now().timestamp())
        output_file = os.path.join(output_dir, f"hourly_summary_{timestamp}.parquet")
        final_df.to_parquet(output_file, index=False)
        print(f"✅ Hourly gold summary for {symbol} saved to {output_file}")
    else:
        print(f"⚠️ No data aggregated for {symbol} on {processing_date}")


def run_hourly_aggregation():
    processing_date = date.today().isoformat()
    #processing_date = "2025-07-08"
    for symbol in symbols:
        aggregate_symbol_hourly(symbol, processing_date)


if __name__ == "__main__":
    run_hourly_aggregation()
