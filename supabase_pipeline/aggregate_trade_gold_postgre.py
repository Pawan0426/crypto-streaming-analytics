# supabase/aggregate_trade_gold_postgre.py

import os
import pandas as pd
from datetime import datetime, timedelta, timezone
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

symbols = ["BTC-USD", "ETH-USD", "ADA-USD", "DOGE-USD"]
time_buckets = ["30min", "1h"]

DATABASE_URL = os.getenv("SUPABASE_DB_URL")
if not DATABASE_URL:
    raise ValueError("❌ Environment variable SUPABASE_DB_URL is not set!")

engine = create_engine(DATABASE_URL)

def aggregate_from_supabase(symbol: str, hours_back=1):
    print(f"🔄 Aggregating {symbol} from Supabase silver layer...")

    # Define time range
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(hours=hours_back)

    query = text("""
        SELECT * FROM crypto_trade_silver
        WHERE symbol = :symbol
          AND time BETWEEN :start_time AND :end_time
    """)

    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={"symbol": symbol, "start_time": start_time, "end_time": end_time})

    if df.empty:
        print(f"⚠️ No silver data for {symbol} in the last {hours_back} hour(s)")
        return

    df["time"] = pd.to_datetime(df["time"], utc=True)
    df.set_index("time", inplace=True)

    for bucket in time_buckets:
        resampled = df.resample(bucket).agg(
            price_min=('price', 'min'),
            price_max=('price', 'max'),
            price_avg=('price', 'mean'),
            total_volume=('size', 'sum'),
            avg_trade_size=('size', 'mean'),
            trade_count=('trade_id', 'count'),
            buy_volume=('side', lambda x: (x == 'buy').sum()),
            sell_volume=('side', lambda x: (x == 'sell').sum())
        ).dropna()

        if resampled.empty:
            continue

        resampled.reset_index(inplace=True)
        resampled = resampled.rename(columns={"time": "fetched_at"})
        resampled["symbol"] = symbol
        resampled["date"] = end_time.date()
        resampled["bucket"] = bucket

        try:
            resampled.to_sql("crypto_trade_aggregates", con=engine, if_exists="append", index=False)
            print(f"✅ Inserted {len(resampled)} rows into gold table for {symbol} ({bucket})")
        except Exception as e:
            print(f"❌ Insert failed for {symbol}, {bucket}: {e}")

def run_gold_trade_aggregation_postgre():
    for symbol in symbols:
        aggregate_from_supabase(symbol, hours_back=1)

if __name__ == "__main__":
    run_gold_trade_aggregation_postgre()
