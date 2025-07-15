# supabase_pipeline/transform_trades_silver_supabase.py

import os
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime, timezone
from dotenv import load_dotenv
from transformation.validation import validate_trade_data  # Reuse your existing validation

load_dotenv()

DATABASE_URL = os.getenv("SUPABASE_DB_URL")
if not DATABASE_URL:
    raise ValueError("❌ SUPABASE_DB_URL not set in .env")

engine = create_engine(DATABASE_URL)
symbols = ["BTC-USD", "ETH-USD", "ADA-USD", "DOGE-USD"]

def transform_symbol(symbol):
    print(f"🔄 Transforming {symbol}...")

    query = text("""
        SELECT * FROM crypto_trade_raw
        WHERE symbol = :symbol
          AND fetched_at >= now() - interval '1 hour'
    """)

    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={"symbol": symbol})

    if df.empty:
        print(f"⚠️ No raw data found for {symbol}")
        return

    try:
        df["price"] = df["price"].astype(float)
        df["size"] = df["size"].astype(float)
        df["time"] = pd.to_datetime(df["time"], utc=True)
        df["fetched_at"] = pd.to_datetime(df["fetched_at"], utc=True)
        df = df.sort_values("time")
    except Exception as e:
        print(f"❌ Error processing {symbol}: {e}")
        return

    issues = validate_trade_data(df, symbol, level="silver")
    if issues:
        print(f"❌ Validation failed for {symbol}:")
        for issue in issues:
            print(f"   - {issue}")
        return

    # Insert into silver table
    try:
        #df.to_sql("crypto_trade_silver", con=engine, if_exists="append", index=False)
        df[["trade_id", "price", "size", "side", "time", "fetched_at", "symbol"]].to_sql(
            "crypto_trade_silver", con=engine, if_exists="append", index=False
        )
        print(f"✅ Inserted {len(df)} rows into crypto_trade_silver for {symbol}")
    except Exception as e:
        print(f"❌ Failed to insert into silver table: {e}")

def run_silver_transformation():
    for symbol in symbols:
        transform_symbol(symbol)

if __name__ == "__main__":
    run_silver_transformation()
