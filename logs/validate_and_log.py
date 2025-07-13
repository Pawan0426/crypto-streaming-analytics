import os
import json
from datetime import date
import pandas as pd
import glob

# Import validation functions
from transformation.validation import validate_trade_data, validate_ticker_data

# -----------------------------
# Base Paths and Config
# -----------------------------
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SILVER_TRADE_DIR = os.path.join(BASE_DIR, "data", "silver", "trades")
TICKER_DIR = os.path.join(BASE_DIR, "data", "silver", "ticker")
GOLD_DIR = os.path.join(BASE_DIR, "data", "gold", "trades")
LOG_DIR = os.path.join(BASE_DIR, "logs")
os.makedirs(LOG_DIR, exist_ok=True)

symbols = ["BTC-USD", "ETH-USD", "ADA-USD", "DOGE-USD"]
processing_date = date.today().isoformat()
log_file_path = os.path.join(LOG_DIR, f"validation_log_{processing_date}.json")
all_issues = {}

# -----------------------------
# ✅ Gold Trade Validation
# -----------------------------
print("🔍 Validating GOLD trade data...")
for symbol in symbols:
    input_dir = os.path.join(GOLD_DIR, processing_date, symbol)
    parquet_files = glob.glob(f"{input_dir}/*.parquet")

    for file in parquet_files:
        df = pd.read_parquet(file)
        issues = validate_trade_data(df, symbol, level="gold")
        if issues:
            all_issues[file] = issues
            print(f"❌ [GOLD] {file}")
        else:
            print(f"✅ [GOLD] {file}")

# -----------------------------
# ✅ Silver Trade Validation
# -----------------------------
print("\n🔍 Validating SILVER trade data...")
for symbol in symbols:
    input_dir = os.path.join(SILVER_TRADE_DIR, processing_date, symbol)
    parquet_files = glob.glob(f"{input_dir}/*.parquet")

    for file in parquet_files:
        df = pd.read_parquet(file)
        issues = validate_trade_data(df, symbol, level="silver")
        if issues:
            all_issues[file] = issues
            print(f"❌ [SILVER] {file}")
        else:
            print(f"✅ [SILVER] {file}")

# -----------------------------
# ✅ Ticker Validation
# -----------------------------
print("\n🔍 Validating TICKER data...")
ticker_files = glob.glob(os.path.join(TICKER_DIR, processing_date, "*.parquet"))
for file in ticker_files:
    df = pd.read_parquet(file)
    symbol = os.path.basename(file).split("_")[0]  # Assumes file starts with symbol
    issues = validate_ticker_data(df, symbol)
    if issues:
        all_issues[file] = issues
        print(f"❌ [TICKER] {file}")
    else:
        print(f"✅ [TICKER] {file}")

# -----------------------------
# Write JSON Log
# -----------------------------
if all_issues:
    with open(log_file_path, "w") as f:
        json.dump(all_issues, f, indent=2)
    print(f"\n📄 Validation issues logged to: {log_file_path}")
else:
    print("\n✅ All data passed validation!")
