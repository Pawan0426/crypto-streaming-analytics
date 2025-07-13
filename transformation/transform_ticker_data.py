import os
import pandas as pd
from datetime import datetime, date
import glob

from transformation.validation import validate_ticker_data


# Define input/output structure
#BRONZE_DIR = "../data/bronze"
#SILVER_DIR = "data/silver"

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
BRONZE_DIR = os.path.join(BASE_DIR, "data", "bronze")
SILVER_DIR = os.path.join(BASE_DIR, "data", "silver")


# Symbols you're processing
symbols = ["BTC-USD", "ETH-USD", "ADA-USD", "DOGE-USD"]

def transform_symbol(symbol, processing_date):
    input_dir = os.path.join(BRONZE_DIR, processing_date, symbol)
    output_dir = os.path.join(SILVER_DIR, processing_date, symbol)
    os.makedirs(output_dir, exist_ok=True)

    # Find all Parquet files
    parquet_files = glob.glob(f"{input_dir}/*.parquet")
    if not parquet_files:
        print(f"No parquet files found for {symbol} on {processing_date}")
        return

    dfs = []
    for file in parquet_files:
        df = pd.read_parquet(file)
        dfs.append(df)

    # Combine all data
    df = pd.concat(dfs, ignore_index=True)

    # Deduplicate by symbol & timestamp (optional)
    df.drop_duplicates(subset=["symbol", "time"], inplace=True)

    # Convert time column to datetime, if not already
    df["time"] = pd.to_datetime(df["time"])
    #df["fetched_at"] = pd.to_datetime(df["fetched_at"])
    df["fetched_at"] = pd.to_datetime(df["fetched_at"], format='mixed', utc=True)

    # Sort chronologically
    df.sort_values(by="time", inplace=True)

    # After cleaning and type casting
    issues = validate_ticker_data(df, symbol)
    if issues:
        print(f"❌ Validation failed for {symbol}:")
        for issue in issues:
            print(f"   - {issue}")
        return  # 🔁 Do not save if validation fails

    # Save cleaned version
    timestamp = int(datetime.now().timestamp())
    output_file = os.path.join(output_dir, f"cleaned_{timestamp}.parquet")
    df.to_parquet(output_file, index=False)
    print(f"Saved transformed {symbol} data to {output_file}")


def run_transformation():
    processing_date = date.today().isoformat()

    for symbol in symbols:
        transform_symbol(symbol, processing_date)


if __name__ == "__main__":
    run_transformation()
