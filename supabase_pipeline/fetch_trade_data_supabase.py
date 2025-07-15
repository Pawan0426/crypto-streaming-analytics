# supabase_pipeline/fetch_trade_data_supabase.py

import os
import requests
from datetime import datetime, timezone
from sqlalchemy import create_engine
from dotenv import load_dotenv
import pandas as pd

# Load env variables (including SUPABASE_DB_URL)
load_dotenv()

DATABASE_URL = os.getenv("SUPABASE_DB_URL")
if not DATABASE_URL:
    raise ValueError("❌ SUPABASE_DB_URL not found in environment variables")

# SQLAlchemy engine
engine = create_engine(DATABASE_URL)

symbols = ["BTC-USD", "ETH-USD", "ADA-USD", "DOGE-USD"]

def fetch_trades(symbol):
    url = f"https://api.exchange.coinbase.com/products/{symbol}/trades"
    try:
        resp = requests.get(url)
        if resp.status_code == 200:
            data = resp.json()
            fetched_at = datetime.now(timezone.utc).isoformat()
            for trade in data:
                trade["fetched_at"] = fetched_at
                trade["symbol"] = symbol
            return data
        else:
            print(f"⚠️ Failed to fetch {symbol} trades, status {resp.status_code}")
    except Exception as e:
        print(f"❌ Exception fetching {symbol}: {e}")
    return []

def push_to_supabase(trade_data):
    if not trade_data:
        return

    df = pd.DataFrame(trade_data)
    if df.empty:
        return

    df["price"] = df["price"].astype(float)
    df["size"] = df["size"].astype(float)
    df["time"] = pd.to_datetime(df["time"], utc=True)
    df["fetched_at"] = pd.to_datetime(df["fetched_at"], utc=True)

    try:
        df.to_sql("crypto_trade_raw", con=engine, if_exists="append", index=False)
        print(f"✅ Inserted {len(df)} rows into crypto_trade_raw")
    except Exception as e:
        print(f"❌ Failed to insert into Supabase: {e}")

def run():
    for symbol in symbols:
        print(f"🔄 Fetching {symbol} trades...")
        trades = fetch_trades(symbol)
        push_to_supabase(trades)

if __name__ == "__main__":
    run()
