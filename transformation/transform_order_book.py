import os
import glob
import pandas as pd
import json
from datetime import datetime, date

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
RAW_DIR = os.path.join(BASE_DIR, "data", "raw", "order_books")
SILVER_DIR = os.path.join(BASE_DIR, "data", "silver", "order_books")

symbols = ["BTC-USD", "ETH-USD", "ADA-USD", "DOGE-USD"]
TOP_N = 5  # Top N bids/asks to keep

def transform_order_book(symbol, processing_date):
    input_dir = os.path.join(RAW_DIR, symbol, processing_date)
    output_dir = os.path.join(SILVER_DIR, symbol, processing_date)
    os.makedirs(output_dir, exist_ok=True)

    json_files = glob.glob(f"{input_dir}/order_book_*.json")
    if not json_files:
        print(f"No order book files found for {symbol} on {processing_date}")
        return

    rows = []

    for file in json_files:
        with open(file, "r") as f:
            data = json.load(f)

        fetched_at = data.get("fetched_at")
        bids = data.get("bids", [])[:TOP_N]
        asks = data.get("asks", [])[:TOP_N]

        for i, bid in enumerate(bids):
            rows.append({
                "fetched_at": fetched_at,
                "side": "bid",
                "level": i + 1,
                "price": float(bid[0]),
                "size": float(bid[1]),
                "symbol": symbol
            })

        for i, ask in enumerate(asks):
            rows.append({
                "fetched_at": fetched_at,
                "side": "ask",
                "level": i + 1,
                "price": float(ask[0]),
                "size": float(ask[1]),
                "symbol": symbol
            })

    df = pd.DataFrame(rows)
    #df["fetched_at"] = pd.to_datetime(df["fetched_at"], utc=True)
    df["fetched_at"] = pd.to_datetime(df["fetched_at"], format='ISO8601', utc=True)
    df.sort_values(by=["fetched_at", "side", "level"], inplace=True)

    timestamp = int(datetime.now().timestamp())
    output_file = os.path.join(output_dir, f"transformed_order_book_{timestamp}.parquet")
    df.to_parquet(output_file, index=False)
    print(f"✅ Transformed order book data for {symbol} saved to {output_file}")

def run_transformation():
    processing_date = date.today().isoformat()
    for symbol in symbols:
        transform_order_book(symbol, processing_date)

if __name__ == "__main__":
    run_transformation()
